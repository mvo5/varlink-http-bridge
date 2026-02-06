// SPDX-License-Identifier: LGPL-2.1-or-later

use log::warn;
use serde_json::{Value, json};
use zlink::idl::{Comment, CustomType, EnumVariant, Field, Interface, Type};

/// Widen a schema to also accept JSON `null`.
///
/// varlink services send an explicit `null` for an unset `?T` field rather
/// than omitting the key, so a schema that only drops the field from
/// `required` still rejects every real reply.
fn make_nullable(schema: Value) -> Value {
    let Value::Object(mut map) = schema else {
        return schema;
    };
    // the empty schema (`any`) already accepts null
    if map.is_empty() {
        return Value::Object(map);
    }
    match map.get("type") {
        Some(Value::String(ty)) => {
            let ty = ty.clone();
            map.insert("type".to_string(), json!([ty, "null"]));
            // a value must still be listed in `enum` to validate, so the
            // widened type alone would not admit null
            if let Some(Value::Array(variants)) = map.get_mut("enum") {
                variants.push(Value::Null);
            }
            Value::Object(map)
        }
        // `$ref` siblings are ignored by many validators, so null has to be
        // expressed as an alternative rather than an extra type
        _ => json!({"anyOf": [Value::Object(map), {"type": "null"}]}),
    }
}

fn type_to_schema(ty: &Type) -> Value {
    match ty {
        Type::Bool => json!({"type": "boolean"}),
        Type::Int => json!({"type": "integer", "format": "int64"}),
        Type::Float => json!({"type": "number"}),
        Type::String => json!({"type": "string"}),
        Type::ForeignObject => json!({"type": "object"}),
        // `any` is any JSON value, not just an object
        Type::Any => json!({}),
        Type::Custom(name) => {
            json!({"$ref": format!("#/components/schemas/{name}")})
        }
        Type::Optional(inner) => make_nullable(type_to_schema(inner.inner())),
        Type::Array(inner) => {
            json!({"type": "array", "items": type_to_schema(inner.inner())})
        }
        Type::Map(inner) => {
            json!({"type": "object", "additionalProperties": type_to_schema(inner.inner())})
        }
        Type::Object(fields) => fields_to_schema(fields.iter()),
        Type::Enum(variants) => {
            let names: Vec<&str> = variants.iter().map(EnumVariant::name).collect();
            json!({"type": "string", "enum": names})
        }
    }
}

fn fields_to_schema<'a>(fields: impl Iterator<Item = &'a Field<'a>>) -> Value {
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();

    for field in fields {
        let mut schema = type_to_schema(field.ty());
        set_description(&mut schema, comments_to_string(field.comments()));
        properties.insert(field.name().to_string(), schema);
        if !matches!(field.ty(), Type::Optional(_)) {
            required.push(json!(field.name()));
        }
    }

    let mut schema = serde_json::Map::new();
    schema.insert("type".to_string(), json!("object"));
    schema.insert("properties".to_string(), Value::Object(properties));
    if !required.is_empty() {
        schema.insert("required".to_string(), Value::Array(required));
    }
    Value::Object(schema)
}

fn comments_to_string<'a>(comments: impl Iterator<Item = &'a Comment<'a>>) -> Option<String> {
    let parts: Vec<&str> = comments.map(Comment::content).collect();
    (!parts.is_empty()).then(|| parts.join("\n"))
}

fn set_description(schema: &mut Value, desc: Option<String>) {
    if let Some(desc) = desc
        && let Value::Object(map) = schema
    {
        map.insert("description".to_string(), json!(desc));
    }
}

/// systemd's IDL comment convention for the `more` flag: a marker on
/// its own doc comment line. systemd itself relies on the exact
/// wording, so it is stable.
const SUPPORTS_MORE_MARKER: &str = "[Supports 'more' flag]";
const REQUIRES_MORE_MARKER: &str = "[Requires 'more' flag]";

/// How a method relates to varlink's `more` flag, derived from the
/// systemd IDL comment convention.
enum MoreFlag {
    /// No `more` support, single JSON response only.
    None,
    /// [`SUPPORTS_MORE_MARKER`]: client may optionally use `more: true`.
    Supports,
    /// [`REQUIRES_MORE_MARKER`]: client must use `more: true`.
    Requires,
}

/// Whole-line match, so prose that merely mentions a marker is not treated
/// as one.
fn is_more_marker(comment: &Comment) -> bool {
    matches!(
        comment.content().trim(),
        SUPPORTS_MORE_MARKER | REQUIRES_MORE_MARKER
    )
}

fn method_more_flag(method: &zlink::idl::Method) -> MoreFlag {
    for c in method.comments() {
        match c.content().trim() {
            REQUIRES_MORE_MARKER => return MoreFlag::Requires,
            SUPPORTS_MORE_MARKER => return MoreFlag::Supports,
            _ => {}
        }
    }
    MoreFlag::None
}

/// Note attached to the response of a method that can stream.
///
/// This belongs on the Response Object: the OAS 3.1 meta-schema forbids
/// unknown keys (`description` among them) inside a Media Type Object.
const JSON_SEQ_NOTE: &str = "Streaming replies use the varlink 'more' flag: \
     request them with Accept: application/json-seq and each reply arrives as \
     an RFC 7464 JSON text sequence record (RS 0x1E + JSON + LF).";

pub fn idl_to_openapi(address: &str, iface: &Interface) -> Value {
    let mut paths = serde_json::Map::new();

    for method in iface.methods() {
        let full_method = format!("{}.{}", iface.name(), method.name());
        let path = format!("/call/{address}/{full_method}");

        let mut operation = serde_json::Map::new();
        // the fully qualified name stays unique when documents for several
        // interfaces of one socket are fed to a single codegen run
        operation.insert("operationId".to_string(), json!(full_method));
        // the marker lines are a varlink-side convention; over HTTP the same
        // information is already carried by the response content types
        if let Some(desc) = comments_to_string(method.comments().filter(|c| !is_more_marker(c))) {
            operation.insert("description".to_string(), json!(desc));
        }
        operation.insert(
            "requestBody".to_string(),
            json!({
                "required": true,
                "content": {
                    "application/json": {
                        "schema": fields_to_schema(method.inputs())
                    }
                }
            }),
        );
        let output_schema = fields_to_schema(method.outputs());
        let more_flag = method_more_flag(method);

        let mut content = serde_json::Map::new();
        if !matches!(more_flag, MoreFlag::Requires) {
            content.insert(
                "application/json".to_string(),
                json!({"schema": output_schema.clone()}),
            );
        }
        if !matches!(more_flag, MoreFlag::None) {
            content.insert(
                "application/json-seq".to_string(),
                json!({"schema": output_schema}),
            );
        }

        let response_description = match more_flag {
            MoreFlag::None => "Successful response".to_string(),
            _ => format!("Successful response\n\n{JSON_SEQ_NOTE}"),
        };

        operation.insert(
            "responses".to_string(),
            json!({
                "200": {
                    "description": response_description,
                    "content": Value::Object(content)
                }
            }),
        );

        let path_item = json!({ "post": Value::Object(operation) });
        if paths.insert(path, path_item).is_some() {
            warn!(
                "{}: duplicate method '{}' in upstream IDL, only the last is described",
                iface.name(),
                method.name()
            );
        }
    }

    let mut schemas = serde_json::Map::new();

    for custom_type in iface.custom_types() {
        let (mut schema, desc) = match custom_type {
            CustomType::Object(obj) => (
                fields_to_schema(obj.fields()),
                comments_to_string(obj.comments()),
            ),
            CustomType::Enum(e) => {
                let names: Vec<&str> = e.variants().map(EnumVariant::name).collect();
                (
                    json!({"type": "string", "enum": names}),
                    comments_to_string(e.comments()),
                )
            }
        };
        set_description(&mut schema, desc);
        schemas.insert(custom_type.name().to_string(), schema);
    }

    for error in iface.errors() {
        let mut schema = fields_to_schema(error.fields());
        set_description(&mut schema, comments_to_string(error.comments()));
        // varlink keeps type and error names apart, OpenAPI does not, so a
        // name used by both would silently resolve to whichever landed last
        if schemas.insert(error.name().to_string(), schema).is_some() {
            warn!(
                "{}: error '{}' shadows a type of the same name in components/schemas",
                iface.name(),
                error.name()
            );
        }
    }

    let mut doc = json!({
        "openapi": "3.1.0",
        "info": {
            "title": iface.name(),
            "version": "0.0.0",
        },
        "paths": paths,
    });

    set_description(&mut doc["info"], comments_to_string(iface.comments()));

    if !schemas.is_empty() {
        doc["components"] = json!({ "schemas": schemas });
    }

    doc
}

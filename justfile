check: check_srv_binary_size check_helper_binary_size
	cargo fmt --check
	cargo clippy -- -W clippy::pedantic

test:
	cargo test

# the httpd service
srv_binary := "target/release/varlink-http-bridge"
# max_size_kb is a bit arbitrary but it should ensure we don't increase size too much
# without noticing (currently at 3.2MB)
srv_max_size := "4 * 1024 * 1024"

# the varlinkctl helper binary so that varlinkctl exec:varlinkctl-helper can talk to http
helper_binary := "target/release/varlinkctl-helper"
helper_max_size := "2 * 1024 * 1024"

[script]
check_srv_binary_size:
	cargo build --release
	max_size_kb="$(({{srv_max_size}} / 1024 ))"
	cur_size_kb=$(( $(stat --format='%s' {{srv_binary}}) / 1024 ))
	echo "release binary: ${cur_size_kb}KB / ${max_size_kb}KB"
	if [ "$cur_size_kb" -gt "$max_size_kb" ]; then
	  echo "ERROR: release binary exceeds limit"
	  exit 1
	fi

[script]
check_helper_binary_size:
	cargo build --release
	max_size_kb="$(({{helper_max_size}} / 1024 ))"
	cur_size_kb=$(( $(stat --format='%s' {{helper_binary}}) / 1024 ))
	echo "release varlinkctl-helper binary: ${cur_size_kb}KB / ${max_size_kb}KB"
	if [ "$cur_size_kb" -gt "$max_size_kb" ]; then
	  echo "ERROR: release varlinkctl-helper binary exceeds limit"
	  exit 1
	fi

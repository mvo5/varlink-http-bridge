binary := "target/release/varlink-http-bridge"
# max_size_kb is a bit arbitrary but it should ensure we don't increase size too much
# without noticing (currently at 3.2MB)
max_size := "4 * 1024 * 1024"

check: check_binary_size
	cargo fmt --check
	cargo clippy -- -W clippy::pedantic

test:
	cargo test

[script]
check_binary_size:
	cargo build --release
	max_size_kb="$(({{max_size}} / 1024 ))"
	cur_size_kb=$(( $(stat --format='%s' {{binary}}) / 1024 ))
	echo "release binary: ${cur_size_kb}KB / ${max_size_kb}KB"
	if [ "$cur_size_kb" -gt "$max_size_kb" ]; then
	  echo "ERROR: release binary exceeds limit"
	  exit 1
	fi

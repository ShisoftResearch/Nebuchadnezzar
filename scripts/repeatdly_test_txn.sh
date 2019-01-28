#!/usr/bin/env bash

#!/usr/bin/env bash

while true
do
cargo test --color=always --package neb --test tests general -- --nocapture
cargo test --color=always --package neb --test tests multi_cell_update  -- --nocapture
cargo test --color=always --package neb --test tests server_isolation  -- --nocapture
done
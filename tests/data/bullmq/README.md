BullMQ 6.5.4's compiled scripts (`rawScripts/` in the BullMQ repository, its
includes inlined), byte for byte as BullMQ sends them: their SHA-1 is what
`src/bullmq.rs` recognises. MIT-licensed (`LICENSE`). The tests run them as
Lua and natively and compare.

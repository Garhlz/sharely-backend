// 仅初始化尚未配置的副本集；已有配置不做重配，不删除数据。
try {
  rs.status();
} catch (error) {
  if (error.code !== 94) throw error;
  rs.initiate({_id: "rs0", members: [{_id: 0, host: "mongo:27017"}]});
}
let ready = false;
for (let attempt = 0; attempt < 60; attempt++) {
  if (db.hello().isWritablePrimary) { ready = true; break; }
  sleep(1000);
}
if (!ready) throw new Error("MongoDB primary election timed out");

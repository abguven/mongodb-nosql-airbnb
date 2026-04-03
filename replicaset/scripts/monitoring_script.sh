#!/bin/bash
while true; do
  mongosh "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs_noscites" --quiet --eval "
    const status = rs.status();
    print('\n=== ' + new Date().toISOString() + ' ===');
    status.members.forEach(m => {
      const icon = m.stateStr === 'PRIMARY' ? '👑' : '📦';
      print(icon + ' ' + m.name + ' : ' + m.stateStr + ' (health: ' + m.health + ')');
    });
  " 2>/dev/null || echo "⚠️  Connection lost...";
  sleep 2;
done
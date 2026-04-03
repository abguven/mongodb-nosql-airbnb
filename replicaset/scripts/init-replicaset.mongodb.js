// ReplicaSet configuration
const config = {
  _id: "rs_noscites",
  members: [
    { _id: 0, host: "mongo1:27017", priority: 10 },
    { _id: 1, host: "mongo2:27017", priority: 1 },
    { _id: 2, host: "mongo3:27017", priority: 1 }
  ]
};

print("=== Initializing ReplicaSet: rs_noscites ===\n");

// Step 1: Check if the ReplicaSet is already initialized
print("[STEP 1] Checking current state...");
try {
  rs.status();
  print("⚠️  ReplicaSet is already initialized!");
  print("Run rs.status() to display details.");
  print("To reset, destroy containers and Docker volumes with 'docker compose down -v'.");
  quit(1);
} catch (e) {

  switch (e.codeName) {

    case "NoReplicationEnabled":
      print("❌ ReplicaSet not enabled on this server (missing --replSet flag).");
      quit(1);

    case "NotYetInitialized":
      print("ℹ️  ReplicaSet enabled but not yet initialized.");
      print("➡️  The script will now run rs.initiate().\n");
      break;

    default:
      print("❌ Unexpected error: " + e.message);
      quit(1);
  }
}

// Step 2: Initialize the ReplicaSet
print("[STEP 2] Initializing ReplicaSet with config:");
print(JSON.stringify(config, null, 2));
try {
  const initResult = rs.initiate(config);
  print("\nrs.initiate() result:");
  print(JSON.stringify(initResult, null, 2));

  if (initResult.ok !== 1) {
    print("❌ Initialization failed!");
    quit(1);
  }
  print("✓ rs.initiate() executed successfully\n");
} catch (e) {
  print("❌ Initialization error: " + e.message);
  quit(1);
}

// Step 3: Wait for Primary election with timeout
print("[STEP 3] Waiting for Primary election...");
print("(timeout after 30 seconds)\n");

let attempts = 0;
const maxAttempts = 30;
let primaryFound = false;

while (attempts < maxAttempts) {
  attempts++;

  try {
    const status = rs.status();

    // Look for a Primary among members
    const primaryMember = status.members.find(m => m.stateStr === "PRIMARY");

    if (primaryMember) {
      print("✓ Primary elected after " + attempts + " second(s)!");
      print("  → " + primaryMember.name + " is now PRIMARY\n");
      primaryFound = true;
      break;
    } else {
      // Display current state of each member
      print("  [" + attempts + "s] Current states:");
      status.members.forEach(m => {
        print("    - " + m.name + ": " + m.stateStr);
      });
    }
  } catch (e) {
    print("  [" + attempts + "s] Waiting... (" + e.message + ")");
  }

  sleep(1000); // Wait 1 second
}

if (!primaryFound) {
  print("\n❌ Timeout! No Primary elected after " + maxAttempts + " seconds.");
  print("Check container logs with: docker compose logs\n");
  quit(1);
}

// Step 4: Display final status
print("[STEP 4] Final ReplicaSet status:\n");
const finalStatus = rs.status();

print("ReplicaSet name : " + finalStatus.set);
print("Member count    : " + finalStatus.members.length);
print("\nMember details:");

finalStatus.members.forEach(m => {
  const icon = m.stateStr === "PRIMARY" ? "👑" : "📦";
  print("  " + icon + " " + m.name);
  print("      State  : " + m.stateStr);
  print("      Health : " + (m.health === 1 ? "OK" : "KO"));
  if (m.stateStr === "PRIMARY") {
    print("      Uptime : " + m.uptime + "s");
  }
  print("");
});

print("=== ✓ ReplicaSet successfully initialized! ===\n");
print("➡️  Connect to individual nodes:");
print("  mongosh --host localhost:27021  (mongo1)");
print("  mongosh --host localhost:27022  (mongo2)");
print("  mongosh --host localhost:27023  (mongo3)");

print("➡️  Connect via ReplicaSet connection string:");
print("  mongodb://localhost:27021,localhost:27022,localhost:27023/?replicaSet=rs_noscites\n");

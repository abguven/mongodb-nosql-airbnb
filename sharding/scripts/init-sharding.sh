#!/bin/bash


# Wait for a ReplicaSet to elect a Primary
wait_for_primary() {
    local host=$1
    local rs_name=$2
    echo "⏳ Waiting for Primary election in $rs_name on $host..."

    # Loop until the JS command returns "true"
    # Using --quiet and --eval to get just the boolean result
    until mongosh --host $host --port 27017 --quiet --eval "try { rs.status().members.some(m => m.stateStr === 'PRIMARY') } catch(e) { false }" | grep -q "true"; do
        printf "."
        sleep 2
    done
    echo ""
    echo "✅ Primary elected for $rs_name!"
}


# Print step headers
echo_step() {
    echo ""
    echo "################################################################"
    echo "👉 $1"
    echo "################################################################"
}

# ==============================================================================
# STEP 1: INITIALIZE REPLICA SETS (Config Server + Shards)
# ==============================================================================

echo_step "1. Initialize Config Server ReplicaSet (noscites_cfg_rs)"
mongosh --host noscites-cfg1 --port 27017 --quiet --eval '
    try {
        var status = rs.status();
        if (status.ok === 1) {
            print("⚠️  Already initialized.");
        }
    } catch (e) {
        if (e.codeName === "NotYetInitialized" || e.message.includes("no replset config")) {
            print("🚀 Initializing...");
            rs.initiate({
                _id: "noscites_cfg_rs",
                configsvr: true,
                members: [
                    { _id: 0, host: "noscites-cfg1:27017" },
                    { _id: 1, host: "noscites-cfg2:27017" },
                    { _id: 2, host: "noscites-cfg3:27017" }
                ]
            });
            print("✅ Initialization successful.");
        } else {
            print("❌ Error: " + e.message);
        }
    }
'

echo_step "2. Initialize Paris Shard (noscites_shard1_rs)"
mongosh --host noscites-shard1-1 --port 27017 --quiet --eval '
    try {
        var status = rs.status();
        if (status.ok === 1) print("⚠️  Already initialized.");
    } catch (e) {
        print("🚀 Initializing...");
        rs.initiate({
            _id: "noscites_shard1_rs",
            members: [
                { _id: 0, host: "noscites-shard1-1:27017" },
                { _id: 1, host: "noscites-shard1-2:27017" },
                { _id: 2, host: "noscites-shard1-3:27017" }
            ]
        });
        print("✅ Initialization successful.");
    }
'

echo_step "3. Initialize Lyon Shard (noscites_shard2_rs)"
mongosh --host noscites-shard2-1 --port 27017 --quiet --eval '
    try {
        var status = rs.status();
        if (status.ok === 1) print("⚠️  Already initialized.");
    } catch (e) {
        print("🚀 Initializing...");
        rs.initiate({
            _id: "noscites_shard2_rs",
            members: [
                { _id: 0, host: "noscites-shard2-1:27017" },
                { _id: 1, host: "noscites-shard2-2:27017" },
                { _id: 2, host: "noscites-shard2-3:27017" }
            ]
        });
        print("✅ Initialization successful.");
    }
'

echo_step "⏳ Waiting for Primary elections..."

wait_for_primary "noscites-cfg1" "noscites_cfg_rs"
wait_for_primary "noscites-shard1-1" "noscites_shard1_rs"
wait_for_primary "noscites-shard2-1" "noscites_shard2_rs"

# Short pause to let mongos detect the topology
sleep 5


# ==============================================================================
# STEP 2: CLUSTER CONFIGURATION (via Mongos)
# ==============================================================================

echo_step "4. Add Shards to the Cluster via Mongos"
mongosh --host noscites-mongos --port 27017 --quiet --eval '
    var shard1 = "noscites_shard1_rs/noscites-shard1-1:27017,noscites-shard1-2:27017,noscites-shard1-3:27017";
    var shard2 = "noscites_shard2_rs/noscites-shard2-1:27017,noscites-shard2-2:27017,noscites-shard2-3:27017";

    try {
        sh.addShard(shard1);
        print("✅ Paris shard added.");
    } catch (e) {
        print("⚠️  Paris shard: " + e.codeName);
    }

    try {
        sh.addShard(shard2);
        print("✅ Lyon shard added.");
    } catch (e) {
        print("⚠️  Lyon shard: " + e.codeName);
    }
'

echo_step "5. Configure Zones and Tags"
mongosh --host noscites-mongos --port 27017 --quiet --eval '
    // Enable sharding (idempotent, catch if already enabled)
    try {
        sh.enableSharding("short_term_rentals");
        print("✅ Sharding enabled on DB.");
    } catch(e) {
        if (e.codeName === "AlreadyEnabled") print("⚠️  Sharding already enabled on DB.");
        else print("ℹ️ Note: " + e.message);
    }

    // Add tags (idempotent: overwrites or does nothing if already exists)
    sh.addShardTag("noscites_shard1_rs", "Paris");
    sh.addShardTag("noscites_shard2_rs", "Lyon");
    print("✅ Tags (Paris/Lyon) applied to shards.");

    // Add tag ranges (idempotent: does nothing if range already exists)
    try {
        sh.addTagRange("short_term_rentals.listings", { city: "Paris" }, { city: "Paris_" }, "Paris");
        print("✅ Paris zone configured.");
    } catch(e) { print("⚠️  Paris zone: " + e.message); }

    try {
        sh.addTagRange("short_term_rentals.listings", { city: "Lyon" }, { city: "Lyon_" }, "Lyon");
        print("✅ Lyon zone configured.");
    } catch(e) { print("⚠️  Lyon zone: " + e.message); }
'

echo_step "6. Enable Sharding on the Collection"
mongosh --host noscites-mongos --port 27017 --quiet --eval '
    try {
        db.getSiblingDB("short_term_rentals").listings.createIndex({ city: 1 });
        print("✅ Index { city: 1 } created (or already exists).");
    } catch (e) {
        print("❌ Index creation error: " + e.message);
    }

    try {
        sh.shardCollection("short_term_rentals.listings", { city: 1 });
        print("✅ Collection sharded successfully!");
    } catch (e) {
        if (e.codeName === "AlreadySharded") {
            print("⚠️  Collection already sharded.");
        } else {
            print("❌ Error: " + e.message);
        }
    }
'

echo_step "CLUSTER CONFIGURATION COMPLETE!"

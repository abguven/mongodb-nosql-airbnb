/* ======================================================
   Project : NosCités — Collection Merge
   Author  : Abdulkadir GUVEN
   Date    : 12 novembre 2025
   Description :
       Merges paris_listings and lyon_listings into a
       unified listings collection. Source collections
       are kept for integration testing.
   ====================================================== */

use("short_term_rentals");

// ------------------------------------------------------
// 1️⃣ Add city field to each source collection
// ------------------------------------------------------

const nb_paris = db.paris_listings.countDocuments();
db.paris_listings.updateMany(
    { city: { $exists: false } },
    { $set: { city: "Paris" } }
);
const checked_paris = db.paris_listings.countDocuments({ city: "Paris" });

if (checked_paris !== nb_paris) {
    print("❌ Paris city field update failed!");
} else {
    print(`✅ Paris: city field added to ${checked_paris} documents.`);
}

const nb_lyon = db.lyon_listings.countDocuments();
db.lyon_listings.updateMany(
    { city: { $exists: false } },
    { $set: { city: "Lyon" } }
);
const checked_lyon = db.lyon_listings.countDocuments({ city: "Lyon" });

if (checked_lyon !== nb_lyon) {
    print("❌ Lyon city field update failed!");
} else {
    print(`✅ Lyon: city field added to ${checked_lyon} documents.`);
}

// ------------------------------------------------------
// 2️⃣ Merge into unified listings collection
// ------------------------------------------------------

db.paris_listings.aggregate([
    { $merge: { into: "listings", whenMatched: "replace", whenNotMatched: "insert" } }
]);

db.lyon_listings.aggregate([
    { $merge: { into: "listings", whenMatched: "replace", whenNotMatched: "insert" } }
]);

// ------------------------------------------------------
// 3️⃣ Verification
// ------------------------------------------------------

const nb_listings = db.listings.countDocuments();
const nb_paris_in = db.listings.countDocuments({ city: "Paris" });
const nb_lyon_in  = db.listings.countDocuments({ city: "Lyon" });

print("\n--- Merge verification ---");
print(`Total listings   : ${nb_listings}  (expected: ${nb_paris + nb_lyon})`);
print(`Paris in listings: ${nb_paris_in}  (expected: ${nb_paris})`);
print(`Lyon  in listings: ${nb_lyon_in}   (expected: ${nb_lyon})`);

if (nb_listings === nb_paris + nb_lyon) {
    print("✅ Merge successful!");
} else {
    print("❌ Count mismatch — check for duplicates or import errors.");
}

// ------------------------------------------------------
// 4️⃣ Cleanup (optional — uncomment after verification)
// ------------------------------------------------------

// Once the merge is verified, drop the source collections:
// db.paris_listings.drop();
// db.lyon_listings.drop();
// print("🗑️ Source collections dropped.");

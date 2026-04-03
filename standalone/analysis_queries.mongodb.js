/* ======================================================
   Project : NosCités — MongoDB Analysis Queries
   Author  : Abdulkadir GUVEN
   Date    : November 2025
   Description :
       Analysis queries run against the short_term_rentals database.
       Goal: explore key metrics (listings, hosts, availability,
       instant bookable, superhosts).
   ====================================================== */

// ------------------------------------------------------
// Connect to the database
// ------------------------------------------------------
use("short_term_rentals");


// EXERCISE 1 -------------------------------------------
// ------------------------------------------------------
// 1️⃣ Total number of documents
// ------------------------------------------------------
// Goal: verify the volume of the database after import.
// Query: count all documents in the collection.
const nb_listings = db.paris_listings.countDocuments();
print(`\n📌 Nombre total de documents : ${nb_listings}`);

// ------------------------------------------------------
// 2️⃣ Number of available listings
// ------------------------------------------------------
// Goal: identify listings currently marked as available.
// Query: filter on has_availability = true.
const nb_available = db.paris_listings.countDocuments({ has_availability: true });
const pct_available = ((nb_available / nb_listings) * 100).toFixed(2);

print(`📌 Nombre de logements disponibles : ${nb_available}`);
print(`📌 Pourcentage de logements disponibles : ${pct_available}%`);


// EXERCISE 2 -------------------------------------------
// ------------------------------------------------------
// 1️⃣ Top 5 property types
// ------------------------------------------------------
// Goal: identify the most common property types (fine-grained).
print("\n📌 Top 5 des types de propriété :");
db.paris_listings.aggregate([
  { $group: { _id: "$property_type", count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 5 }
]).forEach(printjson);


// ------------------------------------------------------
// 1️⃣(Bis) Listing breakdown by room type
// ------------------------------------------------------
// Goal: analyse the proportion of entire homes vs private rooms.
print("\n📌 Répartition par type de chambre :");
db.paris_listings.aggregate([
  { $group:
    {
        _id: "$room_type",
        count: { $sum: 1 } }
    },
  { $sort: { count: -1 } }
]).forEach(printjson);

// ------------------------------------------------------
// 2️⃣ Top 5 listings by number of reviews
// ------------------------------------------------------
print("\n📌 Top 5 annonces avec le plus d'évaluations :");
db.paris_listings.find(
  {},
  { name: 1, number_of_reviews: 1, _id: 0 }
)
.sort({ number_of_reviews: -1 })
.limit(5)
.forEach(printjson);


// 3️⃣ Total number of unique hosts
const nb_hosts = db.paris_listings.distinct("host_id").length;
print(`\n📌 Nombre total d'hôtes différents : ${nb_hosts}`);


// 4️⃣ Number of instantly bookable listings and their share
const nb_bookable = db.paris_listings.countDocuments({ instant_bookable: true });
const pct_bookable = ((nb_bookable / nb_listings) * 100).toFixed(2);

print(`\n📌 Nombre de logements réservables instantanément : ${nb_bookable}`);
print(`📌 Proportion des annonces réservables instantanément : ${pct_bookable}%`);


// ------------------------------------------------------
// 5️⃣ Hosts with more than 100 listings (professional hosts)
// ------------------------------------------------------
const heavy_host_list = db.paris_listings.aggregate([
    {
        $group: {
            _id: {
                    host_id: "$host_id",
                    host_name: "$host_name"
                },
            total: {$sum: 1}}
    },

    {
        $match: { total: { $gt: 100 } }
    },

    { $sort: {total: -1}},
]).toArray();


const pct_heavy_host = ((heavy_host_list.length / nb_hosts) * 100).toFixed(2);

print("\n📌 Hôtes avec plus de 100 annonces :");
heavy_host_list.forEach(doc => printjson(doc));

print(`📌 Ces hôtes représentent ${pct_heavy_host}% des hôtes.`);

// ------------------------------------------------------
// 6️⃣ Total number of superhosts
// ------------------------------------------------------
const nb_superhosts = db.paris_listings
  .distinct("host_id", { host_is_superhost: true })
  .length;

const pct_superhosts = ((nb_superhosts / nb_hosts) * 100).toFixed(2);

print(`\n📌 Nombre total de superhôtes : ${nb_superhosts}`);
print(`📌 Proportion de superhôtes : ${pct_superhosts}%`);


// ------------------------------------------------------
// ✅ End of script
// ------------------------------------------------------
print("\nAll queries executed successfully.");

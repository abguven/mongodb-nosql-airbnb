/* ======================================================
   Projet : NosCités — Création des vues MongoDB
   Auteur : Abdulkadir GUVEN
   Date : 12 novembre 2025
   Description :
       Script centralisant la création des vues utilisées 
       dans Power BI pour l'analyse du dataset short_term_rentals.
       Structure : 3 vues → KPIs, types de propriétés,
       heavy hosts.
   ====================================================== */

use("short_term_rentals");

const CITY = "Paris"

// ------------------------------------------------------
// 1️⃣ View: Global KPIs
// ------------------------------------------------------
db.view_kpis.drop();
db.createView(
    "view_kpis",
    "listings",
    [
        { $match: { city: CITY } },
        {
            $facet: {
                // General stats
                "general": [
                    {
                        $group: {
                            _id: null,
                            total_listings: { $sum: 1 },
                            available_listings: {
                                $sum: { $cond: ["$has_availability", 1, 0] }
                            },
                            instant_bookable_listings: {
                                $sum: { $cond: ["$instant_bookable", 1, 0] }
                            },
                            total_hosts: { $addToSet: "$host_id" },
                            superhosts: {
                                $addToSet: {
                                    $cond: ["$host_is_superhost", "$host_id", null]
                                }
                            }
                        }
                    }
                ],
                // Heavy hosts (>100 listings)
                "heavy": [
                    {
                        $group: {
                            _id: "$host_id",
                            listing_count: { $sum: 1 }
                        }
                    },
                    {
                        $match: {
                            listing_count: { $gt: 100 }
                        }
                    },
                    {
                        $count: "nb_heavy_hosts"
                    }
                ]
            }
        },
        {
            $project: {
                general: { $arrayElemAt: ["$general", 0] },
                heavy: { $arrayElemAt: ["$heavy.nb_heavy_hosts", 0] }
            }
        },
        {
            $project: {
                _id: 0,
                total_listings: "$general.total_listings",
                available_listings: "$general.available_listings",
                pct_disponibles: {
                    $round: [
                        { $multiply: [
                            { $divide: ["$general.available_listings", "$general.total_listings"] },
                            100
                        ]},
                        1
                    ]
                },
                instant_bookable_listings: "$general.instant_bookable_listings",
                pct_instant_bookable: {
                    $round: [
                        { $multiply: [
                            { $divide: ["$general.instant_bookable_listings", "$general.total_listings"] },
                            100
                        ]},
                        1
                    ]
                },
                nb_hosts: { $size: "$general.total_hosts" },
                nb_superhosts: {
                    $size: {
                        $filter: {
                            input: "$general.superhosts",
                            cond: { $ne: ["$$this", null] }
                        }
                    }
                },
                pct_superhosts: {
                    $round: [
                        { $multiply: [
                            { $divide: [
                                { $size: {
                                    $filter: {
                                        input: "$general.superhosts",
                                        cond: { $ne: ["$$this", null] }
                                    }
                                }},
                                { $size: "$general.total_hosts" }
                            ]},
                            100
                        ]},
                        1
                    ]
                },
                nb_heavy_hosts: { $ifNull: ["$heavy", 0] },
                pct_heavy_hosts: {
                    $round: [
                      { $multiply: [
                          { $divide: [
                              { $ifNull: ["$heavy", 0] },
                              { $size: "$general.total_hosts" }
                        ]},
                        100
                    ]},
                    3
                  ]
                }
            }
        }
    ]
);

print("✔️ view_kpis created");


// ------------------------------------------------------
// 2️⃣ View: Listing breakdown by property type
// ------------------------------------------------------
db.view_property_types.drop();
db.createView(
  "view_property_types",
  "listings",
  [
    { $match: { city: CITY } },
    {
      $group: {
        _id: "$property_type",
        listing_count: { $sum: 1 }
      }
    },
    { $sort: { listing_count: -1 } },
    {
      $project: {
        _id: 0,
        property_type: "$_id",
        listing_count: 1
      }
    }
  ]
);
print("✔️ view_property_types created");


// ------------------------------------------------------
// 3️⃣ View: Hosts with more than 100 listings
// ------------------------------------------------------
db.view_heavy_hosts.drop();
db.createView(
  "view_heavy_hosts",
  "listings",
  [
    { $match: { city: CITY } },
    {
      $group: {
        _id: "$host_id",
        host_name: { $first: "$host_name" },
        listing_count: { $sum: 1 },
      }
    },
    {
      $match: {
        listing_count: { $gt: 100 }
      }
    },
    { $sort: { listing_count: -1 } },
    {
      $project: {
        _id: 0,
        host_id: "$_id",
        host_name: 1,
        listing_count: 1,
      }
    }
  ]
);
print("✔️ view_heavy_hosts created");


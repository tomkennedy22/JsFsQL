import { database } from "./database";
import { results } from "./results";
import fs from "fs/promises";
import path from "path";
import { type_connection_init, type_database, type_loose_query, type_table_init } from "./types";
import { distinct, get_from_dict, group_by, index_by, nest_children, yield_nested_children } from "./utils";
import { nested_join } from "./join";

function writeJsonToFile(filePath: string, data: object): void {
    try {
        // Convert the object to a JSON string
        const jsonData = JSON.stringify(data, null, 2);

        // Write JSON string to a file
        fs.writeFile(filePath, jsonData, 'utf8');
        console.log('JSON file has been saved.');
    } catch (error) {
        console.error('An error occurred while writing JSON to file:', error);
    }
}

const test = async () => {
    console.log(`\n\n\n\n\n\n\n Starting new test suite!`)

    let folder_path = path.resolve(__dirname, `../databases/`)
    let db = new database({ dbname: "test_db", folder_path, do_compression: true });
    await db.read_from_file();


    const db_collection_list: type_table_init[] = [
        {
            table_name: "league_season",
            primary_key: "league_season_id",
            indices: ["league_id"],
            delete_key_list: [],
        },
        {
            table_name: "league",
            primary_key: "league_id",
            indices: [],
            delete_key_list: [],
        },
        {
            table_name: "team",
            primary_key: "team_id",
            indices: ["league_id"],
            delete_key_list: [],
        },
        {
            table_name: "team_season",
            primary_key: "team_season_id",
            indices: ["season"],
            delete_key_list: [],
        },
        {
            table_name: "conference",
            primary_key: "conference_id",
            indices: ["league_id"],
            delete_key_list: [],
        },
        {
            table_name: "division",
            primary_key: "division_id",
            indices: [],
            delete_key_list: [],
        },
        {
            table_name: "tier",
            primary_key: "tier_id",
            indices: [],
            delete_key_list: [],
        },
        {
            table_name: "division_season",
            primary_key: "division_season_id",
            indices: ['season'],
            delete_key_list: [],
        },
        {
            table_name: "tier_season",
            primary_key: "tier_season_id",
            indices: ['season'],
            delete_key_list: [],
        },
        {
            table_name: "conference_season",
            primary_key: "conference_season_id",
            indices: ['season'],
            delete_key_list: [],
        }
    ]

    let collection_connection_list: type_connection_init[] = [
        { table_a_name: 'league', table_b_name: 'league_season', join_key: 'league_id', join_type: 'one_to_many' },
        { table_a_name: 'league_season', table_b_name: 'tier_season', join_key: 'league_season_id', join_type: 'one_to_many' },
        { table_a_name: 'tier_season', table_b_name: 'conference_season', join_key: 'tier_season_id', join_type: 'one_to_many' },
        { table_a_name: 'conference_season', table_b_name: 'division_season', join_key: 'conference_season_id', join_type: 'one_to_many' },
        { table_a_name: 'league_season', table_b_name: 'division_season', join_key: 'league_season_id', join_type: 'one_to_many' },
        { table_a_name: 'division_season', table_b_name: 'team_season', join_key: 'division_season_id', join_type: 'one_to_many' },
        { table_a_name: 'team', table_b_name: 'team_season', join_key: 'team_id', join_type: 'one_to_many' },
        { table_a_name: 'conference', table_b_name: 'conference_season', join_key: 'conference_id', join_type: 'one_to_many' },
        { table_a_name: 'division', table_b_name: 'division_season', join_key: 'division_id', join_type: 'one_to_many' },
        { table_a_name: 'tier', table_b_name: 'tier_season', join_key: 'tier_id', join_type: 'one_to_many' },
    ]

    db_collection_list.forEach(function (col_obj) {
        db.add_table(col_obj);
    });

    collection_connection_list.forEach(function (con_obj) {
        db.add_connection(con_obj);
    })

    let conferences = [{
        "world_id": 1,
        "conference_id": 1,
        "conference_name": "Tommy Football Conference",
        "conference_abbreviation": "TFC",
        "league_id": 1,
        "tier_id": 1
    }, {
        "world_id": 1,
        "conference_id": 2,
        "conference_name": "Emmitt Football Conference",
        "conference_abbreviation": "EFC",
        "league_id": 1,
        "tier_id": 1
    }, {
        "world_id": 1,
        "conference_id": 3,
        "conference_name": "Eloise Coast Conference",
        "conference_abbreviation": "ECC",
        "league_id": 2,
        "tier_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 4,
        "conference_name": "Chicago Athletic Conference",
        "conference_abbreviation": "CAC",
        "league_id": 2,
        "tier_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 5,
        "conference_name": "Big 69",
        "conference_abbreviation": "B69",
        "league_id": 2,
        "tier_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 6,
        "conference_name": "Big 420",
        "conference_abbreviation": "B420",
        "league_id": 2,
        "tier_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 7,
        "conference_name": "Mideast Conference",
        "conference_abbreviation": "MEC",
        "league_id": 2,
        "tier_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 8,
        "conference_name": "Mountain Wheast Conference",
        "conference_abbreviation": "MWC",
        "league_id": 2,
        "tier_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 9,
        "conference_name": "Conference NAFTA",
        "conference_abbreviation": "C-NAFTA",
        "league_id": 2,
        "tier_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 10,
        "conference_name": "Sad Belt",
        "conference_abbreviation": "SBC",
        "league_id": 2,
        "tier_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 11,
        "conference_name": "Mid-River Conference",
        "conference_abbreviation": "MRC",
        "league_id": 2,
        "tier_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 12,
        "conference_name": "TBS Independents",
        "conference_abbreviation": "Ind",
        "league_id": 2,
        "tier_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 13,
        "conference_name": "Fern League",
        "conference_abbreviation": "Fern",
        "league_id": 2,
        "tier_id": 3
    },
    {
        "world_id": 1,
        "conference_id": 14,
        "conference_name": "TCS North",
        "conference_abbreviation": "TCS N",
        "league_id": 2,
        "tier_id": 3
    },
    {
        "world_id": 1,
        "conference_id": 15,
        "conference_name": "TCS East",
        "conference_abbreviation": "TCS E",
        "league_id": 2,
        "tier_id": 3
    },
    {
        "world_id": 1,
        "conference_id": 16,
        "conference_name": "TCS South",
        "conference_abbreviation": "TCS S",
        "league_id": 2,
        "tier_id": 3
    },
    {
        "world_id": 1,
        "conference_id": 17,
        "conference_name": "TCS West",
        "conference_abbreviation": "TCS W",
        "league_id": 2,
        "tier_id": 3
    }]

    let conference_seasons = [{
        "conference_id": 1,
        "season": 2023,
        "conference_season_id": 1,
        "tier_season_id": 1
    },
    {
        "conference_id": 2,
        "season": 2023,
        "conference_season_id": 2,
        "tier_season_id": 1
    },
    {
        "conference_id": 3,
        "season": 2023,
        "conference_season_id": 3,
        "tier_season_id": 2
    },
    {
        "conference_id": 4,
        "season": 2023,
        "conference_season_id": 4,
        "tier_season_id": 2
    },
    {
        "conference_id": 5,
        "season": 2023,
        "conference_season_id": 5,
        "tier_season_id": 2
    },
    {
        "conference_id": 6,
        "season": 2023,
        "conference_season_id": 6,
        "tier_season_id": 2
    },
    {
        "conference_id": 7,
        "season": 2023,
        "conference_season_id": 7,
        "tier_season_id": 2
    },
    {
        "conference_id": 8,
        "season": 2023,
        "conference_season_id": 8,
        "tier_season_id": 2
    },
    {
        "conference_id": 9,
        "season": 2023,
        "conference_season_id": 9,
        "tier_season_id": 2
    },
    {
        "conference_id": 10,
        "season": 2023,
        "conference_season_id": 10,
        "tier_season_id": 2
    },
    {
        "conference_id": 11,
        "season": 2023,
        "conference_season_id": 11,
        "tier_season_id": 2
    },
    {
        "conference_id": 12,
        "season": 2023,
        "conference_season_id": 12,
        "tier_season_id": 2
    },
    {
        "conference_id": 13,
        "season": 2023,
        "conference_season_id": 13,
        "tier_season_id": 3
    },
    {
        "conference_id": 14,
        "season": 2023,
        "conference_season_id": 14,
        "tier_season_id": 3
    },
    {
        "conference_id": 15,
        "season": 2023,
        "conference_season_id": 15,
        "tier_season_id": 3
    },
    {
        "conference_id": 16,
        "season": 2023,
        "conference_season_id": 16,
        "tier_season_id": 3
    },
    {
        "conference_id": 17,
        "season": 2023,
        "conference_season_id": 17,
        "tier_season_id": 3
    }]

    let tiers = [{
        "tier_id": 1,
        "tier_name": "TFL",
        "tier_abbreviation": "FBS",
        "tier_color_secondary_hex": "FFFFFF",
        "tier_color_primary_hex": "000000",
        "tier_level": 1,
        "league_id": 1
    },
    {
        "tier_id": 2,
        "tier_name": "FBTS",
        "tier_abbreviation": "FBS",
        "tier_color_secondary_hex": "FFFFFF",
        "tier_color_primary_hex": "000000",
        "tier_level": 1,
        "league_id": 2
    },
    {
        "tier_id": 3,
        "tier_name": "TCS",
        "tier_abbreviation": "FCS",
        "tier_color_secondary_hex": "FFFFFF",
        "tier_color_primary_hex": "000000",
        "tier_level": 2,
        "league_id": 2
    }]

    let tier_seasons = [{
        "tier_id": 1,
        "season": 2023,
        "tier_season_id": 1,
        "league_season_id": 1
    },
    {
        "tier_id": 2,
        "season": 2023,
        "tier_season_id": 2,
        "league_season_id": 2
    },
    {
        "tier_id": 3,
        "season": 2023,
        "tier_season_id": 3,
        "league_season_id": 2
    }]

    let leagues = [
        {
            "league_id": 1,
            "league_name": "Tommy Football League",
            "league_abbreviation": "TFL",
            "league_color_secondary_hex": "FFFFFF",
            "league_color_primary_hex": "000000",
            "league_level": 1
        },
        {
            "league_id": 2,
            "league_name": "College Football",
            "league_abbreviation": "CFB",
            "league_color_secondary_hex": "FFFFFF",
            "league_color_primary_hex": "000000",
            "league_level": 2
        }
    ]

    let league_seasons = [
        {
            "league_season_id": 1,
            "league_id": 1,
            "season": 2023
        },
        {
            "league_season_id": 2,
            "league_id": 2,
            "season": 2023
        }
    ]

    let division_seasons = [
        {
            "division_id": 1,
            "season": 2023,
            "division_season_id": 1,
            "conference_season_id": 1,
            "league_season_id": 1
        },
        {
            "division_id": 2,
            "season": 2023,
            "division_season_id": 2,
            "conference_season_id": 1,
            "league_season_id": 1
        },
        {
            "division_id": 3,
            "season": 2023,
            "division_season_id": 3,
            "conference_season_id": 1,
            "league_season_id": 1
        },
        {
            "division_id": 4,
            "season": 2023,
            "division_season_id": 4,
            "conference_season_id": 1,
            "league_season_id": 1
        },
        {
            "division_id": 5,
            "season": 2023,
            "division_season_id": 5,
            "conference_season_id": 2,
            "league_season_id": 1
        },
        {
            "division_id": 6,
            "season": 2023,
            "division_season_id": 6,
            "conference_season_id": 2,
            "league_season_id": 1
        },
        {
            "division_id": 7,
            "season": 2023,
            "division_season_id": 7,
            "conference_season_id": 2,
            "league_season_id": 1
        },
        {
            "division_id": 8,
            "season": 2023,
            "division_season_id": 8,
            "conference_season_id": 2,
            "league_season_id": 1
        },
        {
            "division_id": 9,
            "season": 2023,
            "division_season_id": 9,
            "conference_season_id": 3,
            "league_season_id": 2
        },
        {
            "division_id": 10,
            "season": 2023,
            "division_season_id": 10,
            "conference_season_id": 4,
            "league_season_id": 2
        },
        {
            "division_id": 15,
            "season": 2023,
            "division_season_id": 11,
            "conference_season_id": 5,
            "league_season_id": 2
        },
        {
            "division_id": 14,
            "season": 2023,
            "division_season_id": 12,
            "conference_season_id": 6,
            "league_season_id": 2
        },
        {
            "division_id": 16,
            "season": 2023,
            "division_season_id": 13,
            "conference_season_id": 7,
            "league_season_id": 2
        },
        {
            "division_id": 12,
            "season": 2023,
            "division_season_id": 14,
            "conference_season_id": 8,
            "league_season_id": 2
        },
        {
            "division_id": 17,
            "season": 2023,
            "division_season_id": 15,
            "conference_season_id": 9,
            "league_season_id": 2
        },
        {
            "division_id": 11,
            "season": 2023,
            "division_season_id": 16,
            "conference_season_id": 10,
            "league_season_id": 2
        },
        {
            "division_id": 13,
            "season": 2023,
            "division_season_id": 17,
            "conference_season_id": 11,
            "league_season_id": 2
        },
        {
            "division_id": 18,
            "season": 2023,
            "division_season_id": 18,
            "conference_season_id": 12,
            "league_season_id": 2
        },
        {
            "division_id": 19,
            "season": 2023,
            "division_season_id": 19,
            "conference_season_id": 13,
            "league_season_id": 2
        },
        {
            "division_id": 21,
            "season": 2023,
            "division_season_id": 20,
            "conference_season_id": 14,
            "league_season_id": 2
        },
        {
            "division_id": 22,
            "season": 2023,
            "division_season_id": 21,
            "conference_season_id": 15,
            "league_season_id": 2
        },
        {
            "division_id": 20,
            "season": 2023,
            "division_season_id": 22,
            "conference_season_id": 16,
            "league_season_id": 2
        },
        {
            "division_id": 23,
            "season": 2023,
            "division_season_id": 23,
            "conference_season_id": 17,
            "league_season_id": 2
        }
    ]

    let divisions = [
        {
            "conference_id": 1,
            "division_name": "East",
            "division_abbreviation": "E",
            "division_id": 1,
            "tier_id": 1,
            "league_id": 1
        },
        {
            "conference_id": 1,
            "division_name": "West",
            "division_abbreviation": "W",
            "division_id": 2,
            "tier_id": 1,
            "league_id": 1
        },
        {
            "conference_id": 1,
            "division_name": "North",
            "division_abbreviation": "N",
            "division_id": 3,
            "tier_id": 1,
            "league_id": 1
        },
        {
            "conference_id": 1,
            "division_name": "South",
            "division_abbreviation": "S",
            "division_id": 4,
            "tier_id": 1,
            "league_id": 1
        },
        {
            "conference_id": 2,
            "division_name": "East",
            "division_abbreviation": "E",
            "division_id": 5,
            "tier_id": 1,
            "league_id": 1
        },
        {
            "conference_id": 2,
            "division_name": "West",
            "division_abbreviation": "W",
            "division_id": 6,
            "tier_id": 1,
            "league_id": 1
        },
        {
            "conference_id": 2,
            "division_name": "North",
            "division_abbreviation": "N",
            "division_id": 7,
            "tier_id": 1,
            "league_id": 1
        },
        {
            "conference_id": 2,
            "division_name": "South",
            "division_abbreviation": "S",
            "division_id": 8,
            "tier_id": 1,
            "league_id": 1
        },
        {
            "conference_id": 3,
            "division_name": "All",
            "division_abbreviation": "All",
            "division_id": 9,
            "tier_id": 2,
            "league_id": 2
        },
        {
            "conference_id": 4,
            "division_name": "All",
            "division_abbreviation": "All",
            "division_id": 10,
            "tier_id": 2,
            "league_id": 2
        },
        {
            "conference_id": 10,
            "division_name": "All",
            "division_abbreviation": "All",
            "division_id": 11,
            "tier_id": 2,
            "league_id": 2
        },
        {
            "conference_id": 8,
            "division_name": "All",
            "division_abbreviation": "All",
            "division_id": 12,
            "tier_id": 2,
            "league_id": 2
        },
        {
            "conference_id": 11,
            "division_name": "All",
            "division_abbreviation": "All",
            "division_id": 13,
            "tier_id": 2,
            "league_id": 2
        },
        {
            "conference_id": 6,
            "division_name": "All",
            "division_abbreviation": "All",
            "division_id": 14,
            "tier_id": 2,
            "league_id": 2
        },
        {
            "conference_id": 5,
            "division_name": "All",
            "division_abbreviation": "All",
            "division_id": 15,
            "tier_id": 2,
            "league_id": 2
        },
        {
            "conference_id": 7,
            "division_name": "All",
            "division_abbreviation": "All",
            "division_id": 16,
            "tier_id": 2,
            "league_id": 2
        },
        {
            "conference_id": 9,
            "division_name": "All",
            "division_abbreviation": "All",
            "division_id": 17,
            "tier_id": 2,
            "league_id": 2
        },
        {
            "conference_id": 12,
            "division_name": "All",
            "division_abbreviation": "All",
            "division_id": 18,
            "tier_id": 2,
            "league_id": 2
        },
        {
            "conference_id": 13,
            "division_name": "All",
            "division_abbreviation": "All",
            "division_id": 19,
            "tier_id": 3,
            "league_id": 2
        },
        {
            "conference_id": 16,
            "division_name": "All",
            "division_abbreviation": "All",
            "division_id": 20,
            "tier_id": 3,
            "league_id": 2
        },
        {
            "conference_id": 14,
            "division_name": "All",
            "division_abbreviation": "All",
            "division_id": 21,
            "tier_id": 3,
            "league_id": 2
        },
        {
            "conference_id": 15,
            "division_name": "All",
            "division_abbreviation": "All",
            "division_id": 22,
            "tier_id": 3,
            "league_id": 2
        },
        {
            "conference_id": 17,
            "division_name": "All",
            "division_abbreviation": "All",
            "division_id": 23,
            "tier_id": 3,
            "league_id": 2
        }
    ]

    let teams = [
        {
            team_id: 1,
            team_name: "Tommy Football Team",
        },
        {
            team_id: 2,
            team_name: "Emmitt Football Team",
        },
        {
            team_id: 3,
            team_name: "Eloise Coast Team",
        },
        {
            team_id: 4,
            team_name: "Chicago Athletic Team",
        },
        {
            team_id: 5,
            team_name: "Big 69 Team",
        },
        {
            team_id: 6,
            team_name: "Big 420 Team",
        },
        {
            team_id: 7,
            team_name: "Mideast Team",
        },
        {
            team_id: 8,
            team_name: "Mountain Wheast Team",
        }
    ]

    let team_seasons = [
        {
            team_id: 1,
            season: 2023,
            team_season_id: 1,
            division_season_id: 1,
        },
        {
            team_id: 2,
            season: 2023,
            team_season_id: 2,
            division_season_id: 1,
        },
        {
            team_id: 3,
            season: 2023,
            team_season_id: 3,
            division_season_id: 1,
        },
        {
            team_id: 4,
            season: 2023,
            team_season_id: 4,
            division_season_id: 1,
        },
        {
            team_id: 5,
            season: 2023,
            team_season_id: 5,
            division_season_id: 2,
        },
        {
            team_id: 6,
            season: 2023,
            team_season_id: 6,
            division_season_id: 2
        },
        {
            team_id: 7,
            season: 2023,
            team_season_id: 7,
            division_season_id: 2
        },
        {
            team_id: 8,
            season: 2023,
            team_season_id: 8,
            division_season_id: 2
        }
    ]

    db.tables.league.insert(leagues);
    db.tables.league_season.insert(league_seasons);

    db.tables.tier.insert(tiers);
    db.tables.tier_season.insert(tier_seasons);

    db.tables.conference.insert(conferences);
    db.tables.conference_season.insert(conference_seasons);

    db.tables.division.insert(divisions);
    db.tables.division_season.insert(division_seasons);

    db.tables.team.insert(teams);
    db.tables.team_season.insert(team_seasons);


    let league_id = 1;
    let season = 2023;

    let new_join_results: any[] = nested_join(db,
        {
            league_season: {
                filter: { league_id, season },
                children: {
                    // league: {},
                    division_season: {
                        name: 'division_seasons',
                        sort: {
                            'conference_season_id': -1,
                            'division.division_name': -1
                        },
                        children: {
                            team_season: {
                                name: 'team_seasons',
                                children: {
                                    team: {}
                                },
                                filter_up: true
                            },
                            division: {}
                        }
                    }
                },
                // find_fn: 'findOne',
            }
        }
    )

    writeJsonToFile('new_join_results.json', new_join_results);


    let test_yield_nested_children_obj = [
        {
            a: {
                b: {
                    c: {
                        phase: { name: 'hi' }
                    },
                    d: {
                        phase: { name: 'hello' }
                    }
                }
            },
            phase: { name: 'bye' },
            phases: [
                { name: 'one' },
                { name: 'two' },
                { name: 'three' }
            ]
        },
        {
            z: {
                phase: { name: 'hi inside z' }
            },
            q: null
        }
    ]

    let test_yield_nested_children = yield_nested_children(test_yield_nested_children_obj, ['phase', 'phases']);

    console.log('test_yield_nested_children', { test_yield_nested_children_obj, test_yield_nested_children })
}

test();


import { database } from "./database";
import { results } from "./results";
import fs from "fs/promises";
import path from "path";
import { type_connection_init, type_database, type_loose_query, type_table_init } from "./types";
import { distinct, get_from_dict, group_by, index_by, nest_children } from "./utils";
import { highest_parent, join } from "./join";

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
    let db = new database({ dbname: "test_db", folder_path, do_compression: false });
    // await db.read_from_file();



    const db_collection_list: type_table_init[] = [
        {
            table_name: "world_season",
            primary_key: "world_season_id",
            partition_keys: [],
            delete_key_list: []
        },
        {
            table_name: "league_season",
            primary_key: "league_season_id",
            partition_keys: ["league_id"],
            index_keys: ["season"],
            delete_key_list: ['league'],
        },
        {
            table_name: "league",
            primary_key: "league_id",
            partition_keys: [],
            delete_key_list: ['league_seasons', 'teams'],
        },
        {
            table_name: "team",
            primary_key: "team_id",
            partition_keys: ["league_id"],
            delete_key_list: ['team_seasons', 'league', 'conference'],
        },
        {
            table_name: "day",
            primary_key: "day_id",
            partition_keys: ["season"],
            delete_key_list: ['period', 'periods'],
        },
        {
            table_name: "period_day",
            primary_key: "period_day_id",
            partition_keys: ["season"],
            delete_key_list: ['period', 'periods', 'day', 'days'],
        },
        {
            table_name: "team_season",
            primary_key: "team_season_id",
            partition_keys: ["season"],
            delete_key_list: ['team', 'team_season_stats', 'team_games', 'person_team_seasons', 'coach_team_seasons'],
        },
        {
            table_name: "team_season_stats",
            primary_key: "team_season_stats_id",
            partition_keys: ["phase_name"],
            delete_key_list: ['team_season'],
        },
        {
            table_name: "person",
            primary_key: "person_id",
            partition_keys: ["name.initials"],
            delete_key_list: ['person_team_seasons'],
        },
        {
            table_name: "person_team_season",
            primary_key: "person_team_season_id",
            partition_keys: ["team_season_id"],
            delete_key_list: ['team_seasons', 'person', 'team_distances', 'potential_ratings'],
        },
        {
            table_name: "conference",
            primary_key: "conference_id",
            partition_keys: ["league_id"],
            delete_key_list: ['conference_season', 'league', 'teams'],
        },
        {
            table_name: "phase",
            primary_key: "phase_id",
            partition_keys: ["season"],
            delete_key_list: ['periods'],
        },
        {
            table_name: "period",
            primary_key: "period_id",
            partition_keys: ["season"],
            delete_key_list: ['phase', 'days', 'period_days', 'period_day'],
        },
        {
            table_name: "team_game",
            primary_key: "team_game_id",
            partition_keys: ["period_id"],
            delete_key_list: ['person_team_games', 'team_season', 'game'],
        },
        {
            table_name: "person_team_game",
            primary_key: "person_team_game_id",
            partition_keys: ["team_game_id"],
            delete_key_list: ['team_game', 'person_team_season'],
        },
        {
            table_name: "game",
            primary_key: "game_id",
            partition_keys: ["period_id"],
            delete_key_list: ['team_games', 'person_team_games', 'period'],
        },
        {
            table_name: "award",
            primary_key: "award_id",
            partition_keys: ["season"],
            delete_key_list: ['person'],
        },
        {
            table_name: "headline",
            primary_key: "headline_id",
            partition_keys: ["period_id"],
            delete_key_list: [],
        },
        {
            table_name: "division",
            primary_key: "division_id",
            partition_keys: [],
            index_keys: ['conference_id'],
            delete_key_list: ['teams', 'conference'],
        },
        {
            table_name: "organization",
            primary_key: "organization_id",
            partition_keys: [],
            delete_key_list: ['teams', 'conference', 'league'],
        },
        {
            table_name: "division_season",
            primary_key: "division_season_id",
            partition_keys: ['season'],
            index_keys: ['division_id'],
            delete_key_list: ['teams', 'conference', 'league'],
        },
        {
            table_name: "organization_season",
            primary_key: "organization_season_id",
            partition_keys: ['season'],
            delete_key_list: ['teams', 'conference', 'league'],
        },
        {
            table_name: "conference_season",
            primary_key: "conference_season_id",
            partition_keys: ['season'],
            delete_key_list: ['teams', 'conference', 'league'],
        },
        {
            table_name: "rivalry",
            primary_key: "rivalry_id",
            partition_keys: [],
            delete_key_list: [],
        }
    ];

    let db_connections: type_connection_init[] = [
        { table_a_name: 'world_season', table_b_name: 'league_season', join_key: 'world_season_id', join_type: 'one_to_many' },
        { table_a_name: 'organization', table_b_name: 'organization_season', join_key: 'organization_id', join_type: 'one_to_many' },
        { table_a_name: 'organization_season', table_b_name: 'league_season', join_key: 'organization_season_id', join_type: 'one_to_many' },
        { table_a_name: 'league', table_b_name: 'league_season', join_key: 'league_id', join_type: 'one_to_many' },
        { table_a_name: 'league_season', table_b_name: 'conference_season', join_key: 'league_season_id', join_type: 'one_to_many' },
        { table_a_name: 'conference', table_b_name: 'conference_season', join_key: 'conference_id', join_type: 'one_to_many' },
        { table_a_name: 'conference_season', table_b_name: 'division_season', join_key: 'conference_season_id', join_type: 'one_to_many' },
        { table_a_name: 'division', table_b_name: 'division_season', join_key: 'division_id', join_type: 'one_to_many' },
        { table_a_name: 'division_season', table_b_name: 'team_season', join_key: 'division_season_id', join_type: 'one_to_many' },
        { table_a_name: 'team', table_b_name: 'team_season', join_key: 'team_id', join_type: 'one_to_many' },
        { table_a_name: 'team_season', table_b_name: 'person_team_season', join_key: 'team_season_id', join_type: 'one_to_many' },
        { table_a_name: 'person', table_b_name: 'person_team_season', join_key: 'person_id', join_type: 'one_to_many' },
        { table_a_name: 'league_season', table_b_name: 'phase', join_key: 'league_season_id', join_type: 'one_to_many' },
        { table_a_name: 'phase', table_b_name: 'period', join_key: 'phase_id', join_type: 'one_to_many' },
        { table_a_name: 'day', table_b_name: 'game', join_key: 'day_id', join_type: 'one_to_many' },
        { table_a_name: 'day', table_b_name: 'period_day', join_key: 'day_id', join_type: 'one_to_many' },
        { table_a_name: 'period', table_b_name: 'period_day', join_key: 'period_id', join_type: 'one_to_many' },
        { table_a_name: 'game', table_b_name: 'team_game', join_key: 'game_id', join_type: 'one_to_many' },
        { table_a_name: 'team_season', table_b_name: 'team_game', join_key: 'team_season_id', join_type: 'one_to_many' },
        { table_a_name: 'person_team_season', table_b_name: 'person_team_game', join_key: 'person_team_season_id', join_type: 'one_to_many' },
        { table_a_name: 'team_game', table_b_name: 'person_team_game', join_key: 'team_game_id', join_type: 'one_to_many' },
    ]

    db_collection_list.forEach(function (col_obj) {
        db.add_table(col_obj);
    });

    db_connections.forEach(function (con_obj) {
        db.add_connection(con_obj);
    })


    let conferences = [{
        "world_id": 1,
        "conference_id": 1,
        "conference_name": "Tommy Football Conference",
        "conference_abbreviation": "TFC",
        "league_id": 1,
        "organization_id": 1
    }, {
        "world_id": 1,
        "conference_id": 2,
        "conference_name": "Emmitt Football Conference",
        "conference_abbreviation": "EFC",
        "league_id": 1,
        "organization_id": 1
    }, {
        "world_id": 1,
        "conference_id": 3,
        "conference_name": "Eloise Coast Conference",
        "conference_abbreviation": "ECC",
        "league_id": 2,
        "organization_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 4,
        "conference_name": "Chicago Athletic Conference",
        "conference_abbreviation": "CAC",
        "league_id": 2,
        "organization_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 5,
        "conference_name": "Big 69",
        "conference_abbreviation": "B69",
        "league_id": 2,
        "organization_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 6,
        "conference_name": "Big 420",
        "conference_abbreviation": "B420",
        "league_id": 2,
        "organization_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 7,
        "conference_name": "Mideast Conference",
        "conference_abbreviation": "MEC",
        "league_id": 2,
        "organization_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 8,
        "conference_name": "Mountain Wheast Conference",
        "conference_abbreviation": "MWC",
        "league_id": 2,
        "organization_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 9,
        "conference_name": "Conference NAFTA",
        "conference_abbreviation": "C-NAFTA",
        "league_id": 2,
        "organization_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 10,
        "conference_name": "Sad Belt",
        "conference_abbreviation": "SBC",
        "league_id": 2,
        "organization_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 11,
        "conference_name": "Mid-River Conference",
        "conference_abbreviation": "MRC",
        "league_id": 2,
        "organization_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 12,
        "conference_name": "TBS Independents",
        "conference_abbreviation": "Ind",
        "league_id": 2,
        "organization_id": 2
    },
    {
        "world_id": 1,
        "conference_id": 13,
        "conference_name": "Fern League",
        "conference_abbreviation": "Fern",
        "league_id": 2,
        "organization_id": 3
    },
    {
        "world_id": 1,
        "conference_id": 14,
        "conference_name": "TCS North",
        "conference_abbreviation": "TCS N",
        "league_id": 2,
        "organization_id": 3
    },
    {
        "world_id": 1,
        "conference_id": 15,
        "conference_name": "TCS East",
        "conference_abbreviation": "TCS E",
        "league_id": 2,
        "organization_id": 3
    },
    {
        "world_id": 1,
        "conference_id": 16,
        "conference_name": "TCS South",
        "conference_abbreviation": "TCS S",
        "league_id": 2,
        "organization_id": 3
    },
    {
        "world_id": 1,
        "conference_id": 17,
        "conference_name": "TCS West",
        "conference_abbreviation": "TCS W",
        "league_id": 2,
        "organization_id": 3
    }]

    let conference_seasons = [{
        "conference_id": 1,
        "season": 2023,
        "conference_season_id": 1,
        "organization_season_id": 1
    },
    {
        "conference_id": 2,
        "season": 2023,
        "conference_season_id": 2,
        "organization_season_id": 1
    },
    {
        "conference_id": 3,
        "season": 2023,
        "conference_season_id": 3,
        "organization_season_id": 2
    },
    {
        "conference_id": 4,
        "season": 2023,
        "conference_season_id": 4,
        "organization_season_id": 2
    },
    {
        "conference_id": 5,
        "season": 2023,
        "conference_season_id": 5,
        "organization_season_id": 2
    },
    {
        "conference_id": 6,
        "season": 2023,
        "conference_season_id": 6,
        "organization_season_id": 2
    },
    {
        "conference_id": 7,
        "season": 2023,
        "conference_season_id": 7,
        "organization_season_id": 2
    },
    {
        "conference_id": 8,
        "season": 2023,
        "conference_season_id": 8,
        "organization_season_id": 2
    },
    {
        "conference_id": 9,
        "season": 2023,
        "conference_season_id": 9,
        "organization_season_id": 2
    },
    {
        "conference_id": 10,
        "season": 2023,
        "conference_season_id": 10,
        "organization_season_id": 2
    },
    {
        "conference_id": 11,
        "season": 2023,
        "conference_season_id": 11,
        "organization_season_id": 2
    },
    {
        "conference_id": 12,
        "season": 2023,
        "conference_season_id": 12,
        "organization_season_id": 2
    },
    {
        "conference_id": 13,
        "season": 2023,
        "conference_season_id": 13,
        "organization_season_id": 3
    },
    {
        "conference_id": 14,
        "season": 2023,
        "conference_season_id": 14,
        "organization_season_id": 3
        },
        {
        "conference_id": 15,
        "season": 2023,
        "conference_season_id": 15,
        "organization_season_id": 3
        },
        {
        "conference_id": 16,
        "season": 2023,
        "conference_season_id": 16,
        "organization_season_id": 3
        },
        {
        "conference_id": 17,
        "season": 2023,
        "conference_season_id": 17,
        "organization_season_id": 3
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
            "conference_season_id": 1
        },
        {
            "division_id": 2,
            "season": 2023,
            "division_season_id": 2,
            "conference_season_id": 1
        },
        {
            "division_id": 3,
            "season": 2023,
            "division_season_id": 3,
            "conference_season_id": 1
        },
        {
            "division_id": 4,
            "season": 2023,
            "division_season_id": 4,
            "conference_season_id": 1
        },
        {
            "division_id": 5,
            "season": 2023,
            "division_season_id": 5,
            "conference_season_id": 2
        },
        {
            "division_id": 6,
            "season": 2023,
            "division_season_id": 6,
            "conference_season_id": 2
        },
        {
            "division_id": 7,
            "season": 2023,
            "division_season_id": 7,
            "conference_season_id": 2
        },
        {
            "division_id": 8,
            "season": 2023,
            "division_season_id": 8,
            "conference_season_id": 2
        },
        {
            "division_id": 9,
            "season": 2023,
            "division_season_id": 9,
            "conference_season_id": 3
        },
        {
            "division_id": 10,
            "season": 2023,
            "division_season_id": 10,
            "conference_season_id": 4
        },
        {
            "division_id": 15,
            "season": 2023,
            "division_season_id": 11,
            "conference_season_id": 5
        },
        {
            "division_id": 14,
            "season": 2023,
            "division_season_id": 12,
            "conference_season_id": 6
        },
        {
            "division_id": 16,
            "season": 2023,
            "division_season_id": 13,
            "conference_season_id": 7
        },
        {
            "division_id": 12,
            "season": 2023,
            "division_season_id": 14,
            "conference_season_id": 8
        },
        {
            "division_id": 17,
            "season": 2023,
            "division_season_id": 15,
            "conference_season_id": 9
        },
        {
            "division_id": 11,
            "season": 2023,
            "division_season_id": 16,
            "conference_season_id": 10
        },
        {
            "division_id": 13,
            "season": 2023,
            "division_season_id": 17,
            "conference_season_id": 11
        },
        {
            "division_id": 18,
            "season": 2023,
            "division_season_id": 18,
            "conference_season_id": 12
        },
        {
            "division_id": 19,
            "season": 2023,
            "division_season_id": 19,
            "conference_season_id": 13
        },
        {
            "division_id": 21,
            "season": 2023,
            "division_season_id": 20,
            "conference_season_id": 14
        },
        {
            "division_id": 22,
            "season": 2023,
            "division_season_id": 21,
            "conference_season_id": 15
        },
        {
            "division_id": 20,
            "season": 2023,
            "division_season_id": 22,
            "conference_season_id": 16
        },
        {
            "division_id": 23,
            "season": 2023,
            "division_season_id": 23,
            "conference_season_id": 17
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

    db.tables.league.insert(leagues);
    db.tables.league_season.insert(league_seasons);

    // db.tables.tier.insert(tiers);
    // db.tables.tier_season.insert(tier_seasons);

    db.tables.conference.insert(conferences);
    db.tables.conference_season.insert(conference_seasons);

    db.tables.division.insert(divisions);
    db.tables.division_season.insert(division_seasons);


    let league_id = 1;
    let season = 2023;
    let query_addons = {
        'league_season': { league_id, season },
    }

    let { results: result } = join(
        db,
        'league_season',
        ['league', 'organization_season', 'organization', 'conference_season', 'conference', 'division_season', 'division'],
        {
            'league_season': { league_id, season },
        }
    );

    let { results: result_2 } = join(
        db,
        'division_season',
        ['league_season', 'league', 'organization_season', 'organization', 'conference_season', 'conference', 'division_season', 'division'],
        // {
        //     'league_season': { league_id, season },
        // },
        query_addons
    );

    // let result = join(db, 'league_season', join_critera);
    // console.log('result', result)
    writeJsonToFile('join_test.json', result);

    // console.log('result_2', result_2)
    writeJsonToFile('join_test_2.json', result_2);

    await db.save_database()

    let hp = highest_parent(db, ['league_season', 'organization_season', 'conference_season', 'division_season', 'team_season'], {})
    console.log('hp', hp)


    let divs = db.tables.division.find({ conference_id: 1 });

    console.log('divs', divs)
}

test();


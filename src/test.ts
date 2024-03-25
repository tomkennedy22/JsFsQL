import { database } from "./database";
import fs from "fs/promises";
import path from "path";
import { type_connection_init, type_join_criteria, type_table_init } from "./types";
import { squeeze_list_of_dicts, unsqueeze_list_of_dicts } from "./squeeze";
import zlib from 'zlib';
import util from 'util';
import { constant_find_fn, nested_join, type_find_fn } from "./join";
import { print_nested_object } from "./utils";

const gunzip = util.promisify(zlib.gunzip);
const gzip = util.promisify(zlib.gzip);


function write_json_to_file(filePath: string, data: object): void {
    try {
        const jsonData = JSON.stringify(data, null, 2);

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
        // {
        //     table_name: "conference",
        //     primary_key: "conference_id",
        //     indices: ["league_id"],
        //     delete_key_list: [],
        // },
        // {
        //     table_name: "division",
        //     primary_key: "division_id",
        //     indices: [],
        //     delete_key_list: [],
        // },
        // {
        //     table_name: "tier",
        //     primary_key: "tier_id",
        //     indices: [],
        //     delete_key_list: [],
        // },
        // {
        //     table_name: "division_season",
        //     primary_key: "division_season_id",
        //     indices: ['season'],
        //     delete_key_list: [],
        // },
        // {
        //     table_name: "tier_season",
        //     primary_key: "tier_season_id",
        //     indices: ['season'],
        //     delete_key_list: [],
        // },
        // {
        //     table_name: "conference_season",
        //     primary_key: "conference_season_id",
        //     indices: ['season'],
        //     delete_key_list: [],
        // },
        {
            table_name: "city",
            primary_key: "city_state",
            indices: ["state_abbreviation"],
            delete_key_list: [],
        },
        {
            table_name: "person",
            primary_key: "person_id",
            indices: [],
            delete_key_list: [],
        }
    ]

    let collection_connection_list: type_connection_init[] = [
        { table_a_name: 'league', table_b_name: 'league_season', join_key: 'league_id', join_type: 'one_to_many' },
        { table_a_name: 'league_season', table_b_name: 'team_season', join_key: 'league_season_id', join_type: 'one_to_many' },
        // { table_a_name: 'tier_season', table_b_name: 'conference_season', join_key: 'tier_season_id', join_type: 'one_to_many' },
        // { table_a_name: 'conference_season', table_b_name: 'division_season', join_key: 'conference_season_id', join_type: 'one_to_many' },
        // { table_a_name: 'league_season', table_b_name: 'division_season', join_key: 'league_season_id', join_type: 'one_to_many' },
        // { table_a_name: 'division_season', table_b_name: 'team_season', join_key: 'division_season_id', join_type: 'one_to_many' },
        { table_a_name: 'team', table_b_name: 'team_season', join_key: 'team_id', join_type: 'one_to_many' },
        // { table_a_name: 'conference', table_b_name: 'conference_season', join_key: 'conference_id', join_type: 'one_to_many' },
        // { table_a_name: 'division', table_b_name: 'division_season', join_key: 'division_id', join_type: 'one_to_many' },
        // { table_a_name: 'tier', table_b_name: 'tier_season', join_key: 'tier_id', join_type: 'one_to_many' },
        { table_a_name: 'city', table_b_name: 'person', join_key: 'city_state', join_type: 'one_to_many' },
        { table_a_name: 'city', table_b_name: 'team', join_key: 'city_state', join_type: 'one_to_many' }
    ]

    db_collection_list.forEach(function (col_obj) {
        db.add_table(col_obj);
    });

    collection_connection_list.forEach(function (con_obj) {
        db.add_connection(con_obj);
    })

    let json_path = path.resolve(__dirname, `../data/city_master.json`);
    let data = JSON.parse(await fs.readFile(json_path, 'utf8'));
    db.tables.city.insert(data);

    let flat_json_file_names = ['league', 'league_season'];
    let obj_json_file_names = ['team', 'team_season', 'person'];

    for (let file_name of flat_json_file_names) {
        let json_path = path.resolve(__dirname, `../data/${file_name}.json`);
        let data = JSON.parse(await fs.readFile(json_path, 'utf8'));
        db.tables[file_name].insert(data);
    }

    for (let file_name of obj_json_file_names) {
        let json_path = path.resolve(__dirname, `../data/${file_name}.json`);
        let data = JSON.parse(await fs.readFile(json_path, 'utf8'));
        db.tables[file_name].insert(Object.values(data));
    }

    await db.save_database();

    let start_ts = Date.now();

    let query_graph_json = {
        league_season: {
            filter: { league_id: 1, season: 2023 },
            find_fn: 'findOne' as type_find_fn,
            children: {
                league: {},
                team_season: {
                    name: 'team_seasons',
                    sort: { power_rank: -1 },
                    children: {
                        team: {
                            children: {
                                city: {},
                            }
                        },
                    }
                }
            }
        }
    }

    let results = nested_join(db, query_graph_json)

    let end_ts = Date.now();
    console.log('Time taken to join:', end_ts - start_ts, 'ms')

    let persons_with_city_json_path = path.resolve(__dirname, `../data/persons_with_city.json`);
    write_json_to_file(persons_with_city_json_path, results);

    // let query_graph_file_path = path.resolve(__dirname, `../data/query_graph.json`);
    // write_json_to_file(query_graph_file_path, query_graph);

}

test();


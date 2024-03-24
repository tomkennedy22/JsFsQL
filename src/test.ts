import { database } from "./database";
import fs from "fs/promises";
import path from "path";
import { type_connection_init, type_table_init } from "./types";
import { squeeze_list_of_dicts, unsqueeze_list_of_dicts } from "./squeeze";
import zlib from 'zlib';
import util from 'util';
import { QueryGraph, nested_join, reroot_graph } from "./join";
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
        // {
        //     table_name: "league_season",
        //     primary_key: "league_season_id",
        //     indices: ["league_id"],
        //     delete_key_list: [],
        // },
        // {
        //     table_name: "league",
        //     primary_key: "league_id",
        //     indices: [],
        //     delete_key_list: [],
        // },
        // {
        //     table_name: "team",
        //     primary_key: "team_id",
        //     indices: ["league_id"],
        //     delete_key_list: [],
        // },
        // {
        //     table_name: "team_season",
        //     primary_key: "team_season_id",
        //     indices: ["season"],
        //     delete_key_list: [],
        // },
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
        // { table_a_name: 'league', table_b_name: 'league_season', join_key: 'league_id', join_type: 'one_to_many' },
        // { table_a_name: 'league_season', table_b_name: 'tier_season', join_key: 'league_season_id', join_type: 'one_to_many' },
        // { table_a_name: 'tier_season', table_b_name: 'conference_season', join_key: 'tier_season_id', join_type: 'one_to_many' },
        // { table_a_name: 'conference_season', table_b_name: 'division_season', join_key: 'conference_season_id', join_type: 'one_to_many' },
        // { table_a_name: 'league_season', table_b_name: 'division_season', join_key: 'league_season_id', join_type: 'one_to_many' },
        // { table_a_name: 'division_season', table_b_name: 'team_season', join_key: 'division_season_id', join_type: 'one_to_many' },
        // { table_a_name: 'team', table_b_name: 'team_season', join_key: 'team_id', join_type: 'one_to_many' },
        // { table_a_name: 'conference', table_b_name: 'conference_season', join_key: 'conference_id', join_type: 'one_to_many' },
        // { table_a_name: 'division', table_b_name: 'division_season', join_key: 'division_id', join_type: 'one_to_many' },
        // { table_a_name: 'tier', table_b_name: 'tier_season', join_key: 'tier_id', join_type: 'one_to_many' },
        { table_a_name: 'city', table_b_name: 'person', join_key: 'city_state', join_type: 'one_to_many' }
    ]

    db_collection_list.forEach(function (col_obj) {
        db.add_table(col_obj);
    });

    collection_connection_list.forEach(function (con_obj) {
        db.add_connection(con_obj);
    })

    // let person_json_path = path.resolve(__dirname, `../data/person.json`);
    // let person_data = JSON.parse(await fs.readFile(person_json_path, 'utf8'));

    // let city_json_path = path.resolve(__dirname, `../data/city_master.json`);
    // let city_data = JSON.parse(await fs.readFile(city_json_path, 'utf8'));

    // db.tables.person.insert(person_data);
    // db.tables.city.insert(city_data);

    await db.save_database();

    let query_graph: any = {
        city: {
            children: {
                person: {
                    filter: {
                        "name": "Jane Doe"
                    },
                    filter_up: true,
                    children: {
                        b: {
                            children: {
                                d: {}
                            }
                        }
                    }
                }
            }
        }
    }

    // let persons_with_city = nested_join(db, query_graph)

    // let persons_with_city_json_path = path.resolve(__dirname, `../data/persons_with_city.json`);
    // write_json_to_file(persons_with_city_json_path, persons_with_city);

    let qg = new QueryGraph({
        a: {
            filter: { helloworld: 'world' },
            children: {
                b: {
                    children: {
                        c: {
                            filter: { filtera: 1, filterb: 2 },
                            children: {
                                d: {
                                },
                                e: {}
                            }
                        }
                    }
                },
                b2: {},
                aa: {}
            }
        }
    })

    // print_nested_object({
    //     qg
    // })


    qg.reroot_from_most_filtered_node();

    // qg.reroot_from_node_key('c')
    qg.orphan_node()

    print_nested_object({ location: 'done', root: qg.root }, 0, 10000)

    let reroot_path = path.resolve(__dirname, `../data/reroot_output.json`);
    write_json_to_file(reroot_path, qg);

    console.log('Graph Stats', qg.graph_stats)


    // print_nested_object({ query_graph, })
    // print_nested_object({ reroot_graph: reroot_graph(query_graph, 'b') })

}

test();


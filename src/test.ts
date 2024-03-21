import { database } from "./database";
import fs from "fs/promises";
import path from "path";
import { type_connection_init, type_table_init } from "./types";
import { squeeze_list_of_dicts, unsqueeze_list_of_dicts } from "./squeeze";
import zlib from 'zlib';
import util from 'util';

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


    let input_file_path = path.resolve(__dirname, `team_season_id_-1.json`);
    let input_file_data = await fs.readFile(input_file_path, 'utf8');
    let input_file_json = JSON.parse(input_file_data);

    let data: { [key: string]: any }[] = Object.values(input_file_json.data);

    let squeezed_data = squeeze_list_of_dicts(data);

    console.log('squeezeed_data', squeezed_data);
    console.log('squeezeed_data stringify', JSON.stringify(squeezed_data));

    let unsqueezeed_data = unsqueeze_list_of_dicts({ key_list: squeezed_data.key_list, squeezed_data: squeezed_data.squeezed_data, all_values_list: squeezed_data.all_values_list });

    console.log('unsqueezeed_data', unsqueezeed_data);

    let output_file_path = path.resolve(__dirname, `test_squeezed_zipped.json`);
    let compressed_data = await gzip(JSON.stringify(squeezed_data));
    const dirname = path.dirname(output_file_path);
    await fs.mkdir(dirname, { recursive: true });

    // Write to a temporary file first
    let tempFilePath = output_file_path + '.tmp';
    await fs.writeFile(tempFilePath, compressed_data);

    // Rename the temporary file to the actual file name (atomic operation)
    await fs.rename(tempFilePath, output_file_path);

    output_file_path = path.resolve(__dirname, `test_unsqueezed_zipped.json`);
    let uncompressed_data = await gzip(JSON.stringify(data));
    await fs.mkdir(dirname, { recursive: true });

    // Write to a temporary file first
    tempFilePath = output_file_path + '.tmp';
    await fs.writeFile(tempFilePath, uncompressed_data);

    // Rename the temporary file to the actual file name (atomic operation)
    await fs.rename(tempFilePath, output_file_path);
}

test();


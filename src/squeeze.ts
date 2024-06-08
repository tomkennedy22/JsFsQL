import { flatten_dict, set_to_dict } from "./utils"
import zlib from 'zlib';
import util from 'util';
import { type_partition } from "./types";

const gunzip = util.promisify(zlib.gunzip);
const gzip = util.promisify(zlib.gzip);

export const squeeze_list_of_dicts = (data: any[]): { key_list: string[], squeezed_data: any[][], all_values_list: any[] } => {

    let all_keys_set = new Set<string>();
    let all_values_set = new Set<any>();
    let flattened_dicts: any[] = [];

    for (let row of data) {
        let flattened_row = flatten_dict(row);
        flattened_dicts.push(flattened_row);
        for (let key of Object.keys(flattened_row)) {
            if (flattened_row[key] != null && flattened_row[key] != undefined) {
                all_keys_set.add(key);
                all_values_set.add(flattened_row[key]);
            }
        }
    }

    let all_values_list = Array.from(all_values_set);
    let all_values_id_map = new Map<any, number>();
    for (let i = 0; i < all_values_list.length; i++) {
        all_values_id_map.set(all_values_list[i], i);
    }

    let flat_data: any[] = [];
    let all_keys_list = Array.from(all_keys_set);

    for (let row of flattened_dicts) {
        let flat_row: any[] = [];
        for (let key of all_keys_list) {
            let val = row[key];
            if (val != null && val != undefined) {
                flat_row.push(all_values_id_map.get(val));
            }
            else {
                flat_row.push(null);
            }
        }
        flat_data.push(flat_row);
    }

    return { key_list: all_keys_list, squeezed_data: flat_data, all_values_list: all_values_list }
}

export const unsqueeze_list_of_dicts = ({ key_list, squeezed_data, all_values_list }: { key_list: string[], squeezed_data: any[][], all_values_list: any[] }): any[] => {

    let unsqueezed_data: any[] = [];

    for (let row of squeezed_data) {
        let new_row = {};
        for (let i = 0; i < key_list.length && i < row.length; i++) {
            let key = key_list[i];
            let val_id = row[i];
            let val = all_values_list[val_id]

            if (val != null && key) {
                set_to_dict(new_row, key, val);
            }
        }
        unsqueezed_data.push(new_row);
    }

    return unsqueezed_data
}

export const compress_partition = async (partition: type_partition): Promise<Buffer> => {

    let data_list = Object.values(partition.data);
    let squeezed_data = squeeze_list_of_dicts(data_list);
    partition.data = squeezed_data;
    let data_string: Buffer = await gzip(JSON.stringify(partition));

    console.log('compress_partition', { partition, squeezed_data, data_string })

    return data_string;
}

export const uncompress_partition = async (compressed_data: Buffer): Promise<type_partition> => {
    const decompressed_data_string = await gunzip(compressed_data);
    const decompressed_data = JSON.parse(decompressed_data_string.toString());

    const { key_list, squeezed_data, all_values_list } = decompressed_data.data;
    const unsqueezed_data_list = unsqueeze_list_of_dicts({ key_list, squeezed_data, all_values_list });

    let data_obj = {}
    let primary_key = decompressed_data.primary_key;
    for (let row of unsqueezed_data_list) {
        let key = row[primary_key];
        set_to_dict(data_obj, key, row);
    }

    const original_partition = {
        ...decompressed_data,
        data: data_obj
    };

    console.log('original_partition', { original_partition, unsqueezed_data_list, decompressed_data });

    return original_partition;
};
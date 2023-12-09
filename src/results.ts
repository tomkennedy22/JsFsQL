import { table } from "./table";
import { type_query, type_results } from "./types";
import { deep_copy, distinct, get_from_dict, set_to_dict } from "./utils";
import { stringify, parse } from 'flatted';

export class results extends Array implements type_results {

    constructor(elements?: any[] | any) {
        if (elements && !Array.isArray(elements)) {
            elements = [elements];
        }
        else if (!elements) {
            elements = [];
        }

        const serialized = stringify(elements);
        const deserialized = parse(serialized);
        super(...deserialized);
    }

    left_join(right_dataset: results | table, join_keys: string | { left_key: string, right_key: string }, map_style: string, map_keys: { left_field?: string, right_field: string }) {

        if (typeof join_keys === 'string') {
            join_keys = { left_key: join_keys, right_key: join_keys };
        }

        let left_key = join_keys.left_key;
        let right_key = join_keys.right_key;

        let left_dataset = this;
        // if (map_style == 'child_array_cross_join'){
        //     left_dataset = left_dataset.;
        // }

        let right_dataset_rows: results;
        if (right_dataset instanceof table) {
            //TODO - speed up by being index-aware w/ parent
            let right_dataset_indices = right_dataset.indices;
            let right_query: type_query = {};
            right_dataset_indices.forEach((index_name: string) => {
                if (left_dataset.first() && left_dataset.first().hasOwnProperty(index_name)) {
                    set_to_dict(right_query, index_name, { $in: distinct(left_dataset.map(row => get_from_dict(row, index_name))) })
                }
            });

            right_dataset_rows = right_dataset.find(right_query) as results;
        }
        else {
            right_dataset_rows = right_dataset as results;
        }

        let left_dataset_groups = this.group_by(left_key);
        let right_dataset_groups = right_dataset_rows.group_by(right_key);
        let resulting_dataset = new results();

        let left_field = map_keys.left_field || left_key;
        let right_field = map_keys.right_field;


        if (map_style === 'cross_join') {
            left_dataset_groups.forEach((left_rows, left_row_key) => {
                let right_rows = get_from_dict(right_dataset_groups, left_row_key) || [null];

                left_rows.forEach((left_row: any) => {
                    right_rows.forEach((right_row: any) => {
                        // ts-ignore
                        let new_row = { [left_field]: left_row, [right_field]: right_row };
                        resulting_dataset.push(new_row);
                    });
                });

            })
        }
        else if (map_style == 'nest_children') {

            let left_dataset = this;

            left_dataset.forEach((left_row: any) => {
                let left_row_value = get_from_dict(left_row, left_key);
                let right_rows = get_from_dict(right_dataset_groups, left_row_value) || [null];
                let new_row = left_row;
                set_to_dict(new_row, right_field, right_rows);
                resulting_dataset.push(new_row);
            })
        }
        else if (map_style == 'nest_child') {

            let left_dataset = this;
            let right_dataset_index = right_dataset_rows.index_by(right_key);
            left_dataset.forEach((left_row: any) => {
                let left_row_key = get_from_dict(left_row, left_key);
                let right_row = get_from_dict(right_dataset_index, left_row_key);
                let new_row = left_row;

                set_to_dict(new_row, right_field, right_row);
                resulting_dataset.push(new_row);
            })
        }
        else {
            throw new Error(`Unknown map_style: ${map_style}`);
        }


        // Return a new results instance with merged rows
        return resulting_dataset;
    }

    make_copy() {
        return new results(this.map(row => deep_copy(row)));
    }

    index_by(index_field: string) {
        let index = new Map();
        for (let row of this) {
            let index_value = get_from_dict(row, index_field);
            // set_to_dict(index, index_value, deep_copy(row));
            set_to_dict(index, index_value, row);
        }

        return index;
    }

    group_by(group_by_field: string): Map<any, any[]> {
        let groups = {};
        for (let row of this) {
            let group_by_value = get_from_dict(row, group_by_field);

            if (!get_from_dict(groups, group_by_value)) {
                set_to_dict(groups, group_by_value, [])
            }
            let group = get_from_dict(groups, group_by_value);
            group.push(row);
        }

        return new Map(Object.entries(groups));;
    }


    first(): any {
        if (this.length == 0) {
            return null;
        }

        return this[0];
    }

}
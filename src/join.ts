import { type_database, type_loose_query, type_results } from "./types";
import { distinct, get_from_dict, index_by, group_by, nest_children, deep_copy, first_element, print_nested_object } from "./utils";


export class QueryNode {

    name: string;
    filter?: type_loose_query;
    sort?: any;
    find_fn?: 'find' | 'findOne';
    filter_up?: boolean;
    children_obj?: { [key: string]: type_join_criteria };
    children_list: QueryNode[];
    parent_node?: QueryNode;

    constructor(init_data: type_join_criteria) {
        this.filter = init_data.filter || {};
        this.sort = init_data.sort || {};
        this.find_fn = init_data.find_fn || 'find';
        this.name = init_data.name || '';
        this.filter_up = init_data.filter_up || false;
        this.children_obj = init_data.children || {};
        this.children_list = [];
    }
}

export class QueryGraph {

    root: QueryNode;
    graph_stats?: {
        most_specific_filter_node: QueryNode;
        depth_from_root: number;
    }

    constructor(init_data: { [key: string]: type_join_criteria }) {
        let root_key = Object.keys(init_data)[0];
        this.root = new QueryNode({ ...init_data[root_key], name: root_key });

        // print_nested_object({ location: 'constructor', init_data, root_key, root: this.root })

        this.build_graph(init_data[root_key], this.root);
        this.build_graph_stats();
    }

    build_graph_stats() {

        let most_specific_filter_count = this.root.filter ? Object.keys(this.root.filter).length : 0;
        let most_specific_filter_node: QueryNode | null = null;
        let depth_from_root = 0;

        let children_list: QueryNode[] = [this.root];

        while (children_list.length > 0) {
            let child = children_list.shift() as QueryNode;

            if (child.filter && Object.keys(child.filter).length > 0) {
                if (!most_specific_filter_node || Object.keys(child.filter).length > most_specific_filter_count) {
                    most_specific_filter_count = Object.keys(child.filter).length;
                    most_specific_filter_node = child;
                }
            }

            children_list.push(...child.children_list);
        }

        this.graph_stats = {
            most_specific_filter_node: most_specific_filter_node as QueryNode,
            depth_from_root: depth_from_root,
        }
    }

    node_as_join_criteria(node: QueryNode): type_join_criteria {

        let children: { [key: string]: type_join_criteria } = {};

        for (let child of node.children_list) {
            children[child.name] = this.node_as_join_criteria(child);
        }

        return {
            filter: node.filter,
            sort: node.sort,
            find_fn: node.find_fn,
            name: node.name,
            filter_up: node.filter_up,
            children: children,
        }

    }

    build_graph(init_data: type_join_criteria, root: QueryNode) {
        // print_nested_object({ location: 'right inside build_graph', root, init_data });

        let children = root.children_obj;

        // print_nested_object({ location: 'right after children', children, root, });

        for (let key in children) {
            let child = children[key];

            // print_nested_object({ location: 'inside key loop', key, child });
            let node = new QueryNode({ ...child, name: key });
            node.parent_node = root;
            root.children_list.push(node);

            if (child.children) {
                this.build_graph(child.children, node);
            }
        }
    }

    reroot_from_node_key(new_root_key: string): void {
        let children_list: QueryNode[] = [...this.root.children_list];
        let new_root: QueryNode | null = null;

        let loop_count = 0;
        while (children_list.length > 0 && !new_root) {
            console.log('children_list', { children_list, loop_count, new_root_key, new_root })
            let child: QueryNode = children_list.shift() as QueryNode;
            if (child.name === new_root_key) {
                new_root = child;
                break;
            }
            children_list.push(...child.children_list);

            loop_count += 1;
        }

        console.log({ location: 'reroot_from_node_key', new_root_key, new_root });
        if (new_root) {
            this.flip_parent_to_child(new_root, undefined);
            this.root = new_root;
        }

        print_nested_object({ location: 'reroot_from_node_key DONE', root: this.root }, 0, 8);
        this.build_graph_stats();

    }

    flip_parent_to_child(node: QueryNode, new_parent_node: QueryNode | undefined): void {
        let parent_node = node.parent_node;
        if (!parent_node) {
            return;
        }

        let parent_children = parent_node.children_list;

        console.log({ location: 'flip_parent_to_child', node, parent_node, parent_children });

        if (parent_node.children_obj && node.name in parent_node.children_obj) {
            delete parent_node.children_obj[node.name];
        }

        let node_index = parent_children.indexOf(node);
        parent_children.splice(node_index, 1);

        node.children_list.push(parent_node);

        if (!node.children_obj) {
            node.children_obj = {};
        }
        node.children_obj[parent_node.name] = this.node_as_join_criteria(parent_node);
        node.parent_node = new_parent_node;

        this.flip_parent_to_child(parent_node, node);
    }


    reroot_from_most_filtered_node(): void {

        let most_specific_filter_node = this.graph_stats?.most_specific_filter_node;
        if (most_specific_filter_node) {
            this.reroot_from_node_key(most_specific_filter_node.name);
        }
    }


    orphan_node(node?: QueryNode): void {

        if (!node) {
            node = this.root;
        }

        for (let child of node.children_list) {
            this.orphan_node(child);
        }

        node.parent_node = undefined;
    }


}



type table_join_results = {
    data: any[];
    indexes: { [key: string]: { [key: string]: any } };
    groups: { [key: string]: { [key: string]: any[] } };
};

type join_results = {
    results: any[];
    tables: { [key: string]: table_join_results };
}


type type_join_criteria = {
    filter?: type_loose_query;
    sort?: any;
    find_fn?: 'find' | 'findOne';
    name?: string;
    filter_up?: boolean;
    children?: { [key: string]: type_join_criteria };
}

export const nested_join = (db: type_database, join_criteria: { [key: string]: type_join_criteria }): type_results => {
    return recurse_nested_join(db, null, [] as type_results, join_criteria);
}


export const recurse_nested_join = (db: type_database, parent_name: string | null, parent_array: type_results, join_criteria_dict: { [key: string]: type_join_criteria }): type_results => {
    for (let table_name in join_criteria_dict) {
        let table = db.tables[table_name];

        let criteria = join_criteria_dict[table_name] || {};
        let find_fn: 'find' | 'findOne' = criteria.find_fn || 'find';
        let filter = criteria.filter || {};
        let name = criteria.name || table_name;

        let parent_connection: any;
        let parent_join_key: string | null = null;
        if (parent_name) {
            parent_connection = table.table_connections[parent_name];
            parent_join_key = parent_connection.join_key;
            if (parent_join_key) {
                let parent_values = distinct(parent_array.map(row => get_from_dict(row, parent_join_key as string))).filter(value => value);

                if (parent_values.length > 0) {
                    filter[parent_join_key] = { $in: parent_values };
                }
            }
        }

        let data: type_results = table.find(filter);
        let data_by_join_key;


        if (criteria.children) {
            let recursed_val = recurse_nested_join(db, table_name, data, criteria.children);
            data = recursed_val;
        }

        if (parent_name && parent_connection && parent_join_key) {
            if (parent_connection.join_type === 'one_to_many' || parent_connection.join_type === 'one_to_one' || find_fn == 'findOne') {
                data_by_join_key = index_by(data, parent_join_key);
            }
            else if (parent_connection.join_type === 'many_to_one') {
                data_by_join_key = group_by(data, parent_join_key);

            }
        }

        if (parent_name && parent_connection && parent_join_key && parent_connection.join_type === 'many_to_one' && criteria.sort) {
            for (let key in data_by_join_key) {
                data_by_join_key[key] = data_by_join_key[key].sort((a: any, b: any) => {
                    for (let sort_key in criteria.sort) {
                        let sort_val = criteria.sort[sort_key];
                        let val_a = get_from_dict(a, sort_key);
                        let val_b = get_from_dict(b, sort_key);
                        if (val_a < val_b) {
                            return sort_val;
                        }
                        else if (val_a > val_b) {
                            return sort_val * -1;
                        }
                    }
                    return 0;
                });
            }
        }

        if (parent_name && parent_connection && parent_array && parent_join_key) {
            parent_array = nest_children(parent_array, data_by_join_key, parent_join_key, name);
            if (criteria.filter_up) {
                parent_array = parent_array.filter(row => row[name] != undefined);
            }
            // parent_array = parent_array.filter(row => row[name] != undefined);
        }
        else {
            parent_array = find_fn == 'find' ? data : first_element(data);
        }
    }

    return parent_array;
}



export const reroot_graph = (originalRoot: { [key: string]: type_join_criteria }, newRootKey: string): { [key: string]: type_join_criteria } => {
    let path: string[] = [];
    let newRoot: type_join_criteria | null = null;

    // Find the path from the original root to the new root
    function findPath(node: type_join_criteria, key: string, currentPath: string[]): boolean {
        if (node.children && key in node.children) {
            path = [...currentPath, key];
            newRoot = node.children[key];
            return true;
        }
        if (node.children) {
            for (let childKey in node.children) {
                if (findPath(node.children[childKey], key, [...currentPath, childKey])) return true;
            }
        }
        return false;
    }

    findPath({ children: originalRoot }, newRootKey, []);

    // Function to recursively flip the graph
    function flip(node: type_join_criteria, parentKey?: string): type_join_criteria {
        const newChildren: { [key: string]: type_join_criteria } = {};
        if (parentKey) {
            newChildren[parentKey] = { children: {} };
        }
        if (node.children) {
            for (let key in node.children) {
                Object.assign(newChildren, flip(node.children[key], key).children);
            }
        }
        node.children = newChildren;
        return node;
    }

    // Initialize flip with the new root and its immediate parent
    if (path.length > 1 && newRoot) { // Ensure there's a parent to the new root
        const parentKey = path[path.length - 2];
        flip(newRoot, parentKey);
        originalRoot[parentKey] = newRoot; // Reassign the modified tree back
    }

    return originalRoot;
}


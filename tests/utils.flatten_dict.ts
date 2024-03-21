import { flatten_dict } from '../src/utils';

describe('flatten_dict function tests', () => {

    test('flatten_dict empty 1', () => {
        expect(flatten_dict([])).toEqual({ key_list: [], data: [[]] });
    });

    test('flatten_dict empty 2', () => {
        expect(flatten_dict([null])).toEqual({ key_list: [], data: [[]] });
    });

    test('flatten_dict empty 3', () => {
        expect(flatten_dict({})).toEqual({ key_list: [], data: [[]] });
    });

})
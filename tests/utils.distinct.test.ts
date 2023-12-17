import { distinct } from '../src/utils';

describe('distinct function tests', () => {

    test('handles array of nulls 1', () => {
        expect(distinct([null])).toEqual([null]);
    });

    test('handles array of nulls 2', () => {
        expect(distinct([null, 0])).toEqual([null, 0]);
    });

    test('handles array of nulls 3', () => {
        expect(distinct([null, null])).toEqual([null]);
    });

    test('handles empty array', () => {
        expect(distinct([])).toEqual([]);
    });

    test('handles empty object', () => {
        expect(distinct([{}])).toEqual([{}]);
    });

    test('handles 2 empty objects', () => {
        expect(distinct([{}, {}])).toEqual([{}]);
    });

    test('handles poplated objects', () => {
        expect(distinct([{ a: 1 }, { a: 2 }, { a: 1 }])).toEqual([{ a: 1 }, { a: 2 }]);
    });

    test('handles poplated objects 2', () => {
        expect(distinct([{ a: 1 }, { a: 2 }])).toEqual([{ a: 1 }, { a: 2 }]);
    });

    test('returns same array if all elements are distinct', () => {
        expect(distinct([1, 2, 3, 4, 5])).toEqual([1, 2, 3, 4, 5]);
    });

    test('removes duplicate numeric elements', () => {
        expect(distinct([1, 2, 2, 3, 4, 4, 5, 5])).toEqual([1, 2, 3, 4, 5]);
    });

    test('removes duplicate string elements', () => {
        expect(distinct(["apple", "banana", "apple", "orange", "banana"])).toEqual(["apple", "banana", "orange"]);
    });

    test('handles mixed type elements and removes duplicates', () => {
        expect(distinct([1, "1", 2, "2", "2", 3, "three"])).toEqual([1, "1", 2, "2", 3, "three"]);
    });

    test('handles different cases', () => {
        expect(distinct(['a', 'A'])).toEqual(['a', 'A']);
    });

});
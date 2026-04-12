package org.howard.edu.lsp.assignment6;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JUnit 5 test class for IntegerSet.
 * Covers normal behavior, edge cases, and error handling.
 */
public class IntegerSetTest {

    private IntegerSet setA;
    private IntegerSet setB;

    @BeforeEach
    void setUp() {
        setA = new IntegerSet();
        setB = new IntegerSet();

        // setA = [1,2,3]
        setA.add(1);
        setA.add(2);
        setA.add(3);

        // setB = [3,4,5]
        setB.add(3);
        setB.add(4);
        setB.add(5);
    }

    // 🔹 Basic Operations

    @Test
    void testAdd_NoDuplicates() {
        setA.add(1); // duplicate
        assertEquals(3, setA.length());
    }

    @Test
    void testAdd_NewElement() {
        setA.add(10);
        assertTrue(setA.contains(10));
        assertEquals(4, setA.length());
    }

    @Test
    void testRemove_Existing() {
        setA.remove(2);
        assertFalse(setA.contains(2));
    }

    @Test
    void testRemove_NonExisting() {
        setA.remove(99); // should not crash
        assertEquals(3, setA.length());
    }

    @Test
    void testContains() {
        assertTrue(setA.contains(1));
        assertFalse(setA.contains(99));
    }

    @Test
    void testClear() {
        setA.clear();
        assertTrue(setA.isEmpty());
    }

    @Test
    void testIsEmpty() {
        IntegerSet empty = new IntegerSet();
        assertTrue(empty.isEmpty());
        assertFalse(setA.isEmpty());
    }

    @Test
    void testLength() {
        assertEquals(3, setA.length());
    }

    // 🔹 Min / Max

    @Test
    void testLargest() {
        assertEquals(3, setA.largest());
    }

    @Test
    void testSmallest() {
        assertEquals(1, setA.smallest());
    }

    @Test
    void testLargest_EmptySet() {
        IntegerSet empty = new IntegerSet();
        assertThrows(RuntimeException.class, empty::largest);
    }

    @Test
    void testSmallest_EmptySet() {
        IntegerSet empty = new IntegerSet();
        assertThrows(RuntimeException.class, empty::smallest);
    }
    
    @Test
    void testLargest_SingleElement() {
        IntegerSet single = new IntegerSet();
        single.add(42);

        assertEquals(42, single.largest());
    }

    @Test
    void testSmallest_SingleElement() {
        IntegerSet single = new IntegerSet();
        single.add(42);

        assertEquals(42, single.smallest());
    }

    // 🔹 Equals

    @Test
    void testEquals_True() {
        IntegerSet copy = new IntegerSet();
        copy.add(3);
        copy.add(2);
        copy.add(1);

        assertTrue(setA.equals(copy));
    }

    @Test
    void testEquals_False() {
        assertFalse(setA.equals(setB));
    }

    @Test
    void testEquals_Null() {
        assertFalse(setA.equals(null));
    }

    // 🔹 Union

    @Test
    void testUnion() {
        IntegerSet result = setA.union(setB);

        assertTrue(result.contains(1));
        assertTrue(result.contains(5));
        assertEquals(5, result.length());
    }

    @Test
    void testUnion_DoesNotModifyOriginal() {
        IntegerSet result = setA.union(setB);

        assertEquals(3, setA.length());
        assertEquals(3, setB.length());
    }

    @Test
    void testUnion_WithEmptySet() {
        IntegerSet empty = new IntegerSet();

        IntegerSet result1 = setA.union(empty);
        IntegerSet result2 = empty.union(setA);

        assertEquals(setA.length(), result1.length());
        assertEquals(setA.length(), result2.length());

        assertTrue(result1.contains(1));
        assertTrue(result1.contains(2));
        assertTrue(result1.contains(3));
    }

    // 🔹 Intersection

    @Test
    void testIntersect() {
        IntegerSet result = setA.intersect(setB);

        assertTrue(result.contains(3));
        assertEquals(1, result.length());
    }

    @Test
    void testIntersect_NoOverlap() {
        IntegerSet c = new IntegerSet();
        c.add(100);

        IntegerSet result = setA.intersect(c);
        assertTrue(result.isEmpty());
    }

    // 🔹 Difference

    @Test
    void testDiff() {
        IntegerSet result = setA.diff(setB);

        assertTrue(result.contains(1));
        assertTrue(result.contains(2));
        assertEquals(2, result.length());
    }

    @Test
    void testDiff_IdenticalSets() {
        IntegerSet copy = new IntegerSet();
        copy.add(1);
        copy.add(2);
        copy.add(3);

        IntegerSet result = setA.diff(copy);

        assertTrue(result.isEmpty());
    }

    // 🔹 Complement

    @Test
    void testComplement() {
        IntegerSet result = setA.complement(setB); // B - A

        assertTrue(result.contains(4));
        assertTrue(result.contains(5));
        assertEquals(2, result.length());
    }

    @Test
    void testComplement_DisjointSets() {
        IntegerSet disjoint = new IntegerSet();
        disjoint.add(100);
        disjoint.add(200);

        IntegerSet result = setA.complement(disjoint);

        assertTrue(result.contains(100));
        assertTrue(result.contains(200));
        assertEquals(2, result.length());
    }

    // 🔹 toString()

    @Test
    void testToString_SortedFormat() {
        IntegerSet set = new IntegerSet();
        set.add(3);
        set.add(1);
        set.add(2);

        assertEquals("[1, 2, 3]", set.toString());
    }

    @Test
    void testToString_Empty() {
        IntegerSet empty = new IntegerSet();
        assertEquals("[]", empty.toString());
    }

}

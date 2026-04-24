package org.howard.edu.lsp.finalexam.question3;


import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

public class GradeCalculatorTest {

    GradeCalculator gc = new GradeCalculator();

    // 1. Test for average()
    @Test
    public void testAverage() {
        assertEquals(80.0, gc.average(80, 80, 80));
    }

    // 2. Test for letterGrade()
    @Test
    public void testLetterGrade() {
        assertEquals("B", gc.letterGrade(85));
    }

    // 3. Test for isPassing()
    @Test
    public void testIsPassing() {
        assertTrue(gc.isPassing(70));
    }

    // 4. Boundary-value test (lower passing boundary)
    @Test
    public void testBoundaryJustFail() {
        assertEquals("F", gc.letterGrade(59.9));
    }

    // 4. Boundary-value test (exact passing boundary)
    @Test
    public void testBoundaryJustPass() {
        assertTrue(gc.isPassing(60.0));
    }

    // 5. Exception test: score too low
    @Test
    public void testExceptionNegativeScore() {
        assertThrows(IllegalArgumentException.class, () -> {
            gc.average(-1, 80, 90);
        });
    }

    // 5. Exception test: score too high
    @Test
    public void testExceptionTooHighScore() {
        assertThrows(IllegalArgumentException.class, () -> {
            gc.average(101, 80, 90);
        });
    }
}

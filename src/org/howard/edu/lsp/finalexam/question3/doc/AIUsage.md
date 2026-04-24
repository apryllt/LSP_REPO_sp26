AI Tools Used:
ChatGPT

Prompts Used (2–5 max):
1. I need JUnit 5 test cases for the following code that has 1. One test for average()
2. One test for letterGrade()
3. One test for isPassing()
4. Two boundary-value tests
5. Two exception tests using assertThrows()
package org.howard.edu.lsp.finalexam.question3; 

public class GradeCalculator {

    public double average(int score1, int score2, int score3) {
        validateScore(score1);
        validateScore(score2);
        validateScore(score3);
        return (score1 + score2 + score3) / 3.0;
    }

    public String letterGrade(double average) {
        if (average >= 90) return "A";
        else if (average >= 80) return "B";
        else if (average >= 70) return "C";
        else if (average >= 60) return "D";
        else return "F";
    }

    public boolean isPassing(double average) {
        return average >= 60;
    }

    private void validateScore(int score) {
        if (score < 0 || score > 100) {
            throw new IllegalArgumentException("Score must be between 0 and 100");
        }
    }
}

2. Cannot find 'org.junit.platform.commons.annotation.Testable' on project build path. JUnit 5 tests can only be run if JUnit 5 is on the build path.

How AI Helped (2–3 sentences):
AI assited with debugging 

Reflection (1–2 sentences):
Better understanding of how JUnit tests are run.


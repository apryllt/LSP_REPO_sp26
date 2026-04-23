package org.howard.edu.lsp.finalexam.question2;

/**
 * Generates a report for a student.
 */
public class StudentReport extends Report {

    private String studentName;
    private double gpa;

    @Override
    protected void loadData() {
        studentName = "John Doe";
        gpa = 3.8;
    }

    @Override
    protected void formatHeader() {
        System.out.println("=== HEADER ===");
        System.out.println("Student Report\n");
    }

    @Override
    protected void formatBody() {
        System.out.println("=== BODY ===");
        System.out.println("Student Name: " + studentName);
        System.out.println("GPA: " + gpa + "\n");
    }

    @Override
    protected void formatFooter() {
        System.out.println("=== FOOTER ===");
        System.out.println("End of Student Report");
    }
}

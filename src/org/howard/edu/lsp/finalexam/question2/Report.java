package org.howard.edu.lsp.finalexam.question2;

/**
 * Defines the template method for generating a report.
 * Subclasses implement specific data loading and formatting steps.
 */
public abstract class Report {

    // Template method: fixed workflow (do not override)
    public final void generateReport() {
        loadData();
        formatHeader();
        formatBody();
        formatFooter();
    }

    protected abstract void loadData();
    protected abstract void formatHeader();
    protected abstract void formatBody();
    protected abstract void formatFooter();
}

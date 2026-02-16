package org.howard.edu.lsp.assignment3;

/**
 * @author Aprille Thomas
 * This class validates product data in CSV format.
 * It checks if the data has the correct number of columns
 * and if the "id" and "price" fields are of the correct data types.
 */
public class ProductValidator {

    private ProductValidator() {
        // Private constructor to prevents incorrect instantiation
    }

    /**
     * Validates the product data.
     * @param values array of string values representing product attributes
     * @return true if the data is valid, false otherwise
     */
    public static boolean isValid(String[] values) {
        // Check if there are exactly four columns
        if (values.length !=4) return false;
        // Check if "id" is a valid integer and "price" is a valid double
        try {
            Integer.parseInt(values[0].trim());
            Double.parseDouble(values[2].trim());
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}

package org.howard.edu.lsp.assignment3;

import java.io.*;

/**
 * @author Aprille Thomas
 * This class processes an ETL (Extract, Transform, Load) pipeline for product data. 
 * It reads product data from a CSV file, 
 * validates and transforms the data using the Product and ProductValidator classes, and 
 * writes the transformed data to a new CSV file. 
 * The class also keeps track of the number of rows processed, transformed, and 
 * skipped, and handles potential IOExceptions that may occur during file operations.
 */
public class ETLProcessor {
    private String inputFilePath;
    private String outputFilePath;
    private int skippedRows = 0;
    private int totalRows = 0;
    private int transformedRows = 0;

    /**
     * Constructor for ETLProcessor.
     * @param inputFilePath path to the input CSV file
     * @param outputFilePath path for the CSV file with transformed data
     */
    public ETLProcessor(String inputFilePath, String outputFilePath) {
        this.inputFilePath = inputFilePath;
        this.outputFilePath = outputFilePath;
    }

    /**
     * Processes the ETL pipeline: 
     * reads from input file, 
     * validates and transforms data, 
     * writes to output file.
     * 
     * Prints summary of the ETL process.
     */
    public void process() {
        try (BufferedReader br = new BufferedReader(new FileReader(this.inputFilePath));
             BufferedWriter bw = new BufferedWriter(new FileWriter(this.outputFilePath))) {
            
            boolean isHeader = true;
            String line;

            while ((line = br.readLine()) != null) {

                //Write header to output file
                if (isHeader){
                    bw.write(line + ",PriceRange");
                    isHeader = false;
                    continue;
                }

                totalRows++;

                line = line.trim();

                // Skip empty rows
                if (line.isEmpty()) {
                    skippedRows++;
                    continue;
                }

                String[] values = line.split(",");

                // Trim whitespace from each value
                for (int i = 0; i < values.length; i++) {
                    values[i] = values[i].trim();
                }

                // Call ProductValidator to check if the row is valid
                if (!ProductValidator.isValid(values)) {
                    skippedRows++;
                    continue;
                }
                
                // Create a Product object and transform it
                Product product = new Product(
                    Integer.parseInt(values[0]), 
                    values[1], 
                    Double.parseDouble(values[2]), 
                    values[3]);

                product.transform();
                
                bw.newLine();
                // Write the transformed product to the output file in CSV format
                bw.write(product.toCSV());
                transformedRows++;
            }

            // Print summary of the ETL process
            System.out.println("Total rows processed: " + totalRows);
            System.out.println("Rows transformed: " + transformedRows);
            System.out.println("Rows skipped: " + skippedRows);
            System.out.println("ETL process complete.Output file created at: " + this.outputFilePath);

        } catch (IOException e) {
            System.err.println("Error: Missing input file " + this.inputFilePath);
        }
    }
}

package org.howard.edu.lsp.assignment3;
/**
 * @author Aprille Thomas
 * This class represents the main entry point 
 * for the ETL (Extract, Transform, Load) pipeline for product data. 
 * 
 * It initializes the ETLProcessor class with the input and 
 * output file paths and starts the ETL
 */
public class ETLPipeline {
    /**
     * Creates an ETLProcessor instance and starts the ETL process.
     * @param args command line arguments (not used)
     */
    public static void main(String[] args) {
        String inputFilePath = "data/products.csv";
        String outputFilePath = "data/transformed_products.csv";

        ETLProcessor processor = new ETLProcessor(inputFilePath, outputFilePath);
        processor.process();
    }
}

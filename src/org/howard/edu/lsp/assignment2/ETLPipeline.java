/*
* Name: Aprille Thomas
*/

package org.howard.edu.lsp.assignment2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class ETLPipeline {
    public static void main(String[] args) {
        String inputFilePath = "org/howard/edu/lsp/assignment2/data/products.csv";
        String outputFilePath = "org/howard/edu/lsp/assignment2/data/transformed_products.csv";
        int skippedRows = 0;
        int totalRows = 0;
        int transformedRows = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath));
             BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilePath))) {
            
            boolean isHeader = true;
            String line;

            while ((line = br.readLine()) != null) {

                //Write header to output file
                if (isHeader){
                    bw.write(line + ",Price Range");
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

                // Separate the values by commas
                String[] values = line.split(",");

                //Skip if not exactly 4 columns
                if (values.length != 4) {
                    skippedRows++;
                    continue;
                }
                
                // Trim whitespace from each value
                for (int i = 0; i < values.length; i++) {
                    values[i] = values[i].trim();
                }
                
                // Check if ProductID and Price are valid numbers
                try {
                    Integer.parseInt(values[0]);
                    Double.parseDouble(values[2]);
                } catch (NumberFormatException e) {
                    skippedRows++;
                    continue;
                }

                // Convert product name to uppercase
                values[1] = values[1].toUpperCase();
                
                
                // Apply 10% discount to elecetronic products
                double price = Double.parseDouble(values[2]);
                if (values[3].equalsIgnoreCase("Electronics")) {
                    price = price * 0.9; 
                }

                // Explicity round-half-up to 2 decimal places for price
                BigDecimal bd = new BigDecimal(price).setScale(2, RoundingMode.HALF_UP);;
                price = bd.doubleValue();
                values[2] = bd.toString();

                
                // Set 'Electronics' to 'Premium Electronics' if price above $500.00
                if (values[3].equalsIgnoreCase("Electronics") && price > 500.00) {
                        values[3] = "Premium Electronics";
                }
                    

                // Set price range for each product
                String priceRange;
                if (price <=10.00) {
                    priceRange = "Low";
                } else if (price <= 100.00) {
                    priceRange = "Medium";
                } else if (price<= 500.00) {
                    priceRange = "High";
                } else {
                    priceRange = "Premium";
                }     
                
                // Add Price Range as a new column
                String[] newValues = new String[values.length + 1];
                System.arraycopy(values, 0, newValues, 0, values.length);
                newValues[values.length] = priceRange;
                values = newValues;


                // Write the transformed row to the output file
                transformedRows++;
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < values.length; i++) {
                    sb.append(values[i]);
                    if (i < values.length - 1) {
                        sb.append(",");
                    }
                }
                bw.newLine();
                bw.write(sb.toString());
                
            }
            

            System.out.println("Total rows encountered: " + totalRows);
            System.out.println("Total transformed rows: " + transformedRows);
            System.out.println("Total skipped rows: " + skippedRows);
            System.out.println("ETL process completed. Transformed data written to " + outputFilePath);
        }catch (IOException e) {
            System.err.println("Error: Missing input file " + inputFilePath);
            return;
        }
    }
}


package org.howard.edu.lsp.assignment3;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author Aprille Thomas
 * This class represents a product with attributes such as 
 * id, name, price, category, and price range. 
 * It includes a method to transform the product's attributes 
 * converts the product name to uppercase,
 * applies a discount based on category,
 * determines the price range,
 * and updates the category for premium electronics. 
 * 
 * The class also contains a method to convert the product's data into CSV format.
 */
public class Product {
    private int id;
    private String name;
    private double price;
    private String category;
    private String priceRange;

    /**
     * Constructor for Product class.
     * @param id product ID
     * @param name product name
     * @param price product price
     * @param category product category
     */
    public Product(int id, String name, double price, String category) {
        this.id = id;
        this.name = name;
        this.price = price;
        this.category = category;
    }

    /**
     * Transforms the product's attributes based on specific rules:
     * 1. Converts the product name to uppercase.
     * 2. Applies a 10% discount if the category is "Electronics".
     * 3. Rounds the price to 2 decimal places.
     * 4. Updates the category to "Premium Electronics" if price is above $500.00.
     * 5. Determines and sets the price range based on price value.
     */
    public void transform() {
        // convert product name to uppercase
        this.name = this.name.toUpperCase();

        // discount price by 10% if category is "Electronics"
        if (this.category.equalsIgnoreCase("Electronics")) {
            this.price = this.price * 0.9;
        }

        // round price to 2 decimal places
        BigDecimal bd = new BigDecimal(this.price).setScale(2, RoundingMode.HALF_UP);
        this.price = bd.doubleValue();

        // set category to "Premium Electronics" if price above $500.00
        if (this.category.equalsIgnoreCase("Electronics") && this.price > 500.00) {
            this.category = "Premium Electronics";
        }  

        // determine price range
        if (this.price <= 10.00) {
            this.priceRange = "Low";
        } else if (this.price <= 100.00) {
            this.priceRange = "Medium";
        } else if (this.price <= 500.00) {
            this.priceRange = "High";
        } else {
            this.priceRange = "Premium";
        }

    }

    /**
     * Converts the product's data into CSV format.
     * @return CSV string representation of the product
     */
    public String toCSV() {
        return String.format("%d,%s,%.2f,%s,%s", this.id, this.name, this.price, this.category, this.priceRange);
    }
}

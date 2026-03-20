package org.howard.edu.lsp.midterm.strategy;

/**
 * Driver class to demonstrate the Strategy Pattern for different customer types.
 * @author Aprille Thomas
 */
public class Driver {

    /**
     * Main method that demonstrates calculating final prices for
     * REGULAR, MEMBER, VIP, and HOLIDAY customers.
     *
     * @param args Command-line arguments (not used).
     */
    
    public static void main(String[] args) {
        double purchasePrice = 100.0;
        PriceCalculator calculator = new PriceCalculator();

        // REGULAR customer
        calculator.setStrategy(new RegularCustomerStrategy());
        System.out.println("REGULAR: " + calculator.calculatePrice(purchasePrice));

        // MEMBER customer
        calculator.setStrategy(new MemberCustomerStrategy());
        System.out.println("MEMBER: " + calculator.calculatePrice(purchasePrice));

        // VIP customer
        calculator.setStrategy(new VIPCustomerStrategy());
        System.out.println("VIP: " + calculator.calculatePrice(purchasePrice));

        // HOLIDAY customer
        calculator.setStrategy(new HolidayCustomerStrategy());
        System.out.println("HOLIDAY: " + calculator.calculatePrice(purchasePrice));
    }
}

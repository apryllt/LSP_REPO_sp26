### Design Evaluation of `PriceCalculator`

1. **Hard-coded customer types and discounts**  
   - The class directly compares strings like `"REGULAR"`, `"MEMBER"`, `"VIP"`, and `"HOLIDAY"`.  
   - Discount values (`0.90`, `0.80`, `0.85`) are embedded in the method.  
   - **Issue:** Adding new customer types or changing discounts requires modifying this class, violating the **Open/Closed Principle** (software should be open for extension but closed for modification).

2. **Poor extensibility**  
   - Introducing new customer tiers (e.g., `"STUDENT"` with 15% discount) requires additional `if` statements.  
   - This makes the code **rigid** and **hard to maintain** as the system grows.

3. **String comparisons are error-prone**  
   - Using `String.equals()` for type checking is not safe; typos can break the logic.  
   - There’s no compile-time check for valid customer types.

4. **Mixing responsibilities**  
   - The class both **decides the discount** and **applies it**.  
   - Violates the **Single Responsibility Principle** — ideally, one class should only have one reason to change.

5. **Scalability and testability issues**  
   - Logic is tightly coupled inside the method, making it difficult to test different discount strategies independently.

**Impact of hard-coded values**  
- Changing discounts or adding promotions requires editing the method directly.  
- The method will grow cluttered with more `if` statements as new rules are introduced.  
- Typographical errors in strings could lead to subtle bugs.

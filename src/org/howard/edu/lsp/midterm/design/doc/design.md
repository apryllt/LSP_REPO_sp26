## Overview
The redesign introduces an Order class to privately store the necessary order data. Responsibilities are distributed such that PricingService handles tax and discounts, OrderRepository handles storage, NotificationService handles emails, ReceiptService handles receipts, and Logger handles logging. The OrderProcessor now orchestrates these services rather than performing them directly.  
The redesign separates concerns, improves cohesion, reduces coupling through abstraction, and centralizes coordination while distributing responsibilities across focused classes.

## Class: Order
### Responsibilities
- Store order data (customerName, email, item, price)
- Provide controlled access to data (getters)
### Collaborators
- OrderService
- PricingService

---

## Class: OrderProcessor
### Responsibilities
- Coordinate the overall order processing workflow
- Delegate tasks to appropriate services
### Collaborators
- Order
- PricingService
- OrderRepository
- NotificationService
- ReceiptService
- Logger

---

## Class: PricingService
### Responsibilities
- Calculate tax
- Apply discounts
- Compute total price
### Collaborators
- Order

---

## Class: OrderRepository
### Responsibilities
- Save order data to storage (e.g., file or database)
### Collaborators
- Order

---

## Class: NotificationService
### Responsibilities
- Send confirmation emails to customers
### Collaborators
- Order

---

## Class: ReceiptService
### Responsibilities
- Generate and print/display receipts
### Collaborators
- Order
- PricingService

---

## Class: Logger
### Responsibilities
- Log order processing activities
### Collaborators
- OrderService

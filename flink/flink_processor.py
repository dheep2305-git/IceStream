def validate_transaction(transaction):
    """
    Validate incoming transaction data.
    """

    errors = []

    # Check transaction ID
    if not transaction.get("transaction_id"):
        errors.append("Missing transaction_id")

    # Check customer ID
    if not transaction.get("customer_id"):
        errors.append("Missing customer_id")

    # Check product
    if not transaction.get("product"):
        errors.append("Missing product")

    # Check amount
    amount = transaction.get("amount")

    if amount is None:
        errors.append("Missing amount")
    elif amount <= 0:
        errors.append("Invalid amount")

    # Check payment method
    if not transaction.get("payment_method"):
        errors.append("Missing payment_method")

    if errors:
        return False, errors

    return True, []


def process_transaction(transaction):

    is_valid, errors = validate_transaction(transaction)

    if is_valid:

        print("✅ GOOD DATA")
        print(transaction)

        return "GOOD", transaction

    else:

        print("❌ BAD DATA")
        print("Errors:", errors)
        print(transaction)

        return "BAD", {
            "transaction": transaction,
            "errors": errors
        }


if __name__ == "__main__":

    sample_transaction = {
        "transaction_id": 1,
        "customer_id": "C1001",
        "product": "Laptop",
        "amount": 45000,
        "payment_method": "UPI"
    }

    process_transaction(sample_transaction)
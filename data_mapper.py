"""
data_mapper.py — Loads CSV files from the SAP O2C dataset and normalizes them.
Column names are matched to the actual exported CSVs from the JSONL source.
"""
import pandas as pd
import os


class DataMapper:
    def __init__(self, data_path: str):
        self.data_path = data_path

    def _read(self, filename: str) -> pd.DataFrame:
        path = os.path.join(self.data_path, filename)
        try:
            df = pd.read_csv(path, dtype=str)
            df = df.where(pd.notna(df), None)
            return df
        except Exception as e:
            print(f"  [DataMapper] Could not read {filename}: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------------ #
    def load_customers(self) -> pd.DataFrame:
        df = self._read("business_partners.csv")
        if df.empty:
            return pd.DataFrame(columns=["customer_id", "customer_name"])
        df = df.rename(columns={
            "businessPartner": "customer_id",
            "businessPartnerFullName": "customer_name",
            "customer": "customer_number",
        })
        df["customer_id"] = df["customer_id"].fillna(df.get("customer_number", ""))
        return df

    def load_products(self) -> pd.DataFrame:
        df = self._read("products.csv")
        desc = self._read("product_descriptions.csv")
        if df.empty:
            return pd.DataFrame(columns=["product_id", "product_name"])
        df = df.rename(columns={"product": "product_id"})
        if not desc.empty:
            desc_en = desc[desc["language"] == "EN"][["product", "productDescription"]]
            desc_en = desc_en.rename(columns={"product": "product_id", "productDescription": "product_name"})
            df = df.merge(desc_en, on="product_id", how="left")
        else:
            df["product_name"] = df["product_id"]
        return df

    def load_orders(self) -> pd.DataFrame:
        df = self._read("sales_order_headers.csv")
        if df.empty:
            return pd.DataFrame(columns=["order_id", "customer_id", "total_amount"])
        df = df.rename(columns={
            "salesOrder": "order_id",
            "soldToParty": "customer_id",
            "totalNetAmount": "total_amount",
            "creationDate": "order_date",
            "overallDeliveryStatus": "delivery_status",
            "transactionCurrency": "currency",
        })
        return df

    def load_order_items(self) -> pd.DataFrame:
        df = self._read("sales_order_items.csv")
        if df.empty:
            return pd.DataFrame(columns=["order_item_id", "order_id", "product_id", "quantity", "net_amount"])
        df = df.rename(columns={
            "salesOrderItem": "order_item_id",
            "salesOrder": "order_id",
            "material": "product_id",
            "requestedQuantity": "quantity",
            "netAmount": "net_amount",
            "productionPlant": "plant",
        })
        return df

    def load_deliveries(self) -> pd.DataFrame:
        df = self._read("outbound_delivery_headers.csv")
        if df.empty:
            return pd.DataFrame(columns=["delivery_id", "shipping_point", "goods_movement_status"])
        df = df.rename(columns={
            "deliveryDocument": "delivery_id",
            "shippingPoint": "shipping_point",
            "overallGoodsMovementStatus": "goods_movement_status",
            "overallPickingStatus": "picking_status",
            "creationDate": "delivery_date",
        })
        return df

    def load_delivery_items(self) -> pd.DataFrame:
        df = self._read("outbound_delivery_items.csv")
        if df.empty:
            return pd.DataFrame(columns=["delivery_item_id", "delivery_id", "order_id", "order_item_id", "plant"])
        df = df.rename(columns={
            "deliveryDocumentItem": "delivery_item_id",
            "deliveryDocument": "delivery_id",
            "referenceSdDocument": "order_id",      # ← SO that originated the delivery
            "referenceSdDocumentItem": "order_item_id",
            "actualDeliveryQuantity": "quantity",
            "plant": "plant",
        })
        return df

    def load_invoices(self) -> pd.DataFrame:
        df = self._read("billing_document_headers.csv")
        if df.empty:
            return pd.DataFrame(columns=["invoice_id", "document_number", "customer_id", "amount", "accounting_document"])
        df = df.rename(columns={
            "billingDocument": "invoice_id",
            "soldToParty": "customer_id",
            "totalNetAmount": "amount",
            "accountingDocument": "accounting_document",
            "billingDocumentDate": "invoice_date",
            "billingDocumentIsCancelled": "is_cancelled",
            "transactionCurrency": "currency",
        })
        # Keep document_number == invoice_id
        df["document_number"] = df["invoice_id"]
        return df

    def load_invoice_items(self) -> pd.DataFrame:
        df = self._read("billing_document_items.csv")
        if df.empty:
            return pd.DataFrame(columns=["invoice_item_id", "invoice_id", "product_id", "quantity", "net_amount"])
        df = df.rename(columns={
            "billingDocumentItem": "invoice_item_id",
            "billingDocument": "invoice_id",
            "material": "product_id",
            "billingQuantity": "quantity",
            "netAmount": "net_amount",
            "referenceSdDocument": "delivery_id",   # ← delivery that was billed
        })
        return df

    def load_payments(self) -> pd.DataFrame:
        df = self._read("payments_accounts_receivable.csv")
        if df.empty:
            return pd.DataFrame(columns=["payment_id", "accounting_document", "customer_id", "amount", "clearing_doc"])
        df = df.rename(columns={
            "accountingDocumentItem": "payment_id",
            "accountingDocument": "accounting_document",
            "customer": "customer_id",
            "amountInTransactionCurrency": "amount",
            "clearingAccountingDocument": "clearing_doc",
            "clearingDate": "payment_date",
            "postingDate": "posting_date",
        })
        # Composite unique key
        df["payment_id"] = df["accounting_document"].fillna("") + "_" + df["payment_id"].fillna("")
        return df

    def load_journal_entries(self) -> pd.DataFrame:
        df = self._read("journal_entry_items.csv")
        if df.empty:
            return pd.DataFrame(columns=["document_number", "reference_document", "customer_id", "amount"])
        df = df.rename(columns={
            "accountingDocument": "document_number",
            "referenceDocument": "reference_document",   # ← billing document
            "customer": "customer_id",
            "amountInTransactionCurrency": "amount",
            "postingDate": "posting_date",
            "accountingDocumentType": "doc_type",
            "glAccount": "gl_account",
            "clearingAccountingDocument": "clearing_doc",
            "clearingDate": "clearing_date",
            "companyCode": "company_code",
            "fiscalYear": "fiscal_year",
        })
        return df
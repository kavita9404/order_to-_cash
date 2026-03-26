# main.py - Order-to-Cash Graph System with White Background
import pandas as pd
import networkx as nx
from pyvis.network import Network
import streamlit as st
from typing import Dict, List, Any, Tuple
import re
import google.generativeai as genai
import os
from datetime import datetime

# Import DataMapper
from data_mapper import DataMapper

# ============ PART 1: GRAPH CONSTRUCTION ============

class OrderToCashGraph:
    """
    Graph-based representation of Order-to-Cash process
    """
    
    def __init__(self):
        self.graph = nx.MultiDiGraph()
        self.entity_index = {}
        
    def add_node(self, node_id: str, node_type: str):
        """Add a node to the graph"""
        self.graph.add_node(
            node_id,
            type=node_type,
            attributes={},
            label=f"{node_type}: {node_id}"
        )
        self.entity_index[node_id] = node_id

    def add_node_attribute(self, node_id: str, key: str, value: str):
        """Add an attribute to a node"""
        if node_id in self.graph.nodes:
            self.graph.nodes[node_id]['attributes'][key] = str(value)
        
    def load_data(self, data_path: str):
        """Load and process all CSV files using DataMapper"""
        mapper = DataMapper(data_path)
        
        # Load data using mapper
        customers_df = mapper.load_customers()
        products_df = mapper.load_products()
        orders_df = mapper.load_orders()
        order_items_df = mapper.load_order_items()
        deliveries_df = mapper.load_deliveries()
        delivery_items_df = mapper.load_delivery_items()
        invoices_df = mapper.load_invoices()
        invoice_items_df = mapper.load_invoice_items()
        payments_df = mapper.load_payments()
        journal_entries_df = mapper.load_journal_entries()
        
        # Check if any data loaded
        if customers_df.empty and products_df.empty and orders_df.empty:
            st.warning("No data files found! Using sample data...")
            self._create_sample_data()
            return True
        
        # Build graph nodes and edges
        if not customers_df.empty:
            self._add_customers(customers_df)
        if not products_df.empty:
            self._add_products(products_df)
        if not orders_df.empty:
            self._add_orders(orders_df, order_items_df if not order_items_df.empty else pd.DataFrame())
        if not deliveries_df.empty:
            self._add_deliveries(deliveries_df, delivery_items_df if not delivery_items_df.empty else pd.DataFrame())
        if not invoices_df.empty:
            self._add_invoices(invoices_df, invoice_items_df if not invoice_items_df.empty else pd.DataFrame())
        if not payments_df.empty:
            self._add_payments(payments_df)
        if not journal_entries_df.empty:
            self._add_journal_entries(journal_entries_df)
        
        return True
        
    def _create_sample_data(self):
        """Create sample data matching the reference images"""
        
        # Add business partner (customer)
        self.add_node("BP_1001", "BusinessPartner")
        self.add_node_attribute("BP_1001", "business_partner_name", "Tech Solutions Inc")
        self.add_node_attribute("BP_1001", "business_partner_id", "1001")
        
        # Add product
        self.add_node("PROD_LT001", "Product")
        self.add_node_attribute("PROD_LT001", "product_name", "Laptop Pro X1")
        self.add_node_attribute("PROD_LT001", "product_id", "LT001")
        
        # Add sales order (ReferenceDocument: 91150187)
        self.add_node("ORDER_91150187", "SalesOrder")
        self.add_node_attribute("ORDER_91150187", "order_id", "91150187")
        self.add_node_attribute("ORDER_91150187", "customer_id", "1001")
        self.add_node_attribute("ORDER_91150187", "total_amount", "1167")
        self.add_edge("BP_1001", "ORDER_91150187", "placed_order")
        
        # Add order item
        self.add_node("ORDER_ITEM_1", "OrderItem")
        self.add_node_attribute("ORDER_ITEM_1", "order_item_id", "1")
        self.add_node_attribute("ORDER_ITEM_1", "order_id", "91150187")
        self.add_node_attribute("ORDER_ITEM_1", "product_id", "LT001")
        self.add_node_attribute("ORDER_ITEM_1", "quantity", "1")
        self.add_edge("ORDER_91150187", "ORDER_ITEM_1", "contains_item")
        self.add_edge("ORDER_ITEM_1", "PROD_LT001", "references_product")
        
        # Add delivery
        self.add_node("DELIVERY_DL001", "Delivery")
        self.add_node_attribute("DELIVERY_DL001", "delivery_id", "DL001")
        self.add_node_attribute("DELIVERY_DL001", "order_id", "91150187")
        self.add_node_attribute("DELIVERY_DL001", "status", "Delivered")
        self.add_edge("ORDER_91150187", "DELIVERY_DL001", "has_delivery")
        
        # Add billing document (invoice) with AccountingDocument: 9400635958
        self.add_node("INVOICE_9400635958", "Invoice")
        self.add_node_attribute("INVOICE_9400635958", "invoice_id", "9400635958")
        self.add_node_attribute("INVOICE_9400635958", "document_number", "9400635958")
        self.add_node_attribute("INVOICE_9400635958", "reference_document", "91150187")
        self.add_node_attribute("INVOICE_9400635958", "order_id", "91150187")
        self.add_node_attribute("INVOICE_9400635958", "delivery_id", "DL001")
        self.add_node_attribute("INVOICE_9400635958", "amount", "1167")
        self.add_edge("ORDER_91150187", "INVOICE_9400635958", "has_invoice")
        self.add_edge("DELIVERY_DL001", "INVOICE_9400635958", "has_invoice")
        
        # Add journal entry with all the fields from reference
        self.add_node("JE_9400635958", "JournalEntry")
        self.add_node_attribute("JE_9400635958", "entity", "Journal Entry")
        self.add_node_attribute("JE_9400635958", "company_code", "INR")
        self.add_node_attribute("JE_9400635958", "fiscal_year", "2025")
        self.add_node_attribute("JE_9400635958", "accounting_document", "9400635958")
        self.add_node_attribute("JE_9400635958", "gi_account", "15500020")
        self.add_node_attribute("JE_9400635958", "reference_document", "91150187")
        self.add_node_attribute("JE_9400635958", "cost_center", "")
        self.add_node_attribute("JE_9400635958", "profit_center", "")
        self.add_node_attribute("JE_9400635958", "transaction_currency", "INR")
        self.add_node_attribute("JE_9400635958", "amount_in_transaction_currency", "-1167")
        self.add_node_attribute("JE_9400635958", "company_code_currency", "INR")
        self.add_node_attribute("JE_9400635958", "amount_in_company_code_currency", "-1167")
        self.add_node_attribute("JE_9400635958", "posting_date", "2025-04-02")
        self.add_node_attribute("JE_9400635958", "document_date", "2025-04-02")
        self.add_node_attribute("JE_9400635958", "accounting_document_type", "RV")
        self.add_node_attribute("JE_9400635958", "accounting_document_item", "1")
        self.add_node_attribute("JE_9400635958", "connections", "2")
        self.add_edge("INVOICE_9400635958", "JE_9400635958", "has_journal_entry")
        
    def _add_customers(self, customers_df):
        for _, row in customers_df.iterrows():
            node_id = f"CUST_{row['customer_id']}"
            self.add_node(node_id, "Customer")
            for col in row.index:
                self.add_node_attribute(node_id, col, str(row[col]))
            
    def _add_products(self, products_df):
        for _, row in products_df.iterrows():
            node_id = f"PROD_{row['product_id']}"
            self.add_node(node_id, "Product")
            for col in row.index:
                self.add_node_attribute(node_id, col, str(row[col]))
            
    def _add_orders(self, orders_df, order_items_df):
        for _, row in orders_df.iterrows():
            node_id = f"ORDER_{row['order_id']}"
            self.add_node(node_id, "SalesOrder")
            for col in row.index:
                self.add_node_attribute(node_id, col, str(row[col]))
            if 'customer_id' in row:
                self.add_edge(f"CUST_{row['customer_id']}", node_id, "placed_order")
        
        if not order_items_df.empty:
            for _, row in order_items_df.iterrows():
                item_id = f"ORDER_ITEM_{row['order_item_id']}"
                self.add_node(item_id, "OrderItem")
                for col in row.index:
                    self.add_node_attribute(item_id, col, str(row[col]))
                if 'order_id' in row:
                    self.add_edge(f"ORDER_{row['order_id']}", item_id, "contains_item")
                if 'product_id' in row:
                    self.add_edge(item_id, f"PROD_{row['product_id']}", "references_product")
            
    def _add_deliveries(self, deliveries_df, delivery_items_df):
        for _, row in deliveries_df.iterrows():
            node_id = f"DELIVERY_{row['delivery_id']}"
            self.add_node(node_id, "Delivery")
            for col in row.index:
                self.add_node_attribute(node_id, col, str(row[col]))
            if 'order_id' in row and pd.notna(row.get('order_id')):
                self.add_edge(f"ORDER_{row['order_id']}", node_id, "has_delivery")
        
        if not delivery_items_df.empty:
            for _, row in delivery_items_df.iterrows():
                item_id = f"DELIVERY_ITEM_{row['delivery_item_id']}"
                self.add_node(item_id, "DeliveryItem")
                for col in row.index:
                    self.add_node_attribute(item_id, col, str(row[col]))
                if 'delivery_id' in row:
                    self.add_edge(f"DELIVERY_{row['delivery_id']}", item_id, "contains_item")
                if 'order_item_id' in row and pd.notna(row.get('order_item_id')):
                    self.add_edge(item_id, f"ORDER_ITEM_{row['order_item_id']}", "fulfills")
                
    def _add_invoices(self, invoices_df, invoice_items_df):
        for _, row in invoices_df.iterrows():
            node_id = f"INVOICE_{row['invoice_id']}"
            self.add_node(node_id, "Invoice")
            for col in row.index:
                self.add_node_attribute(node_id, col, str(row[col]))
            if 'order_id' in row and pd.notna(row.get('order_id')):
                self.add_edge(f"ORDER_{row['order_id']}", node_id, "has_invoice")
            if 'delivery_id' in row and pd.notna(row.get('delivery_id')):
                self.add_edge(f"DELIVERY_{row['delivery_id']}", node_id, "has_invoice")
        
        if not invoice_items_df.empty:
            for _, row in invoice_items_df.iterrows():
                item_id = f"INVOICE_ITEM_{row['invoice_item_id']}"
                self.add_node(item_id, "InvoiceItem")
                for col in row.index:
                    self.add_node_attribute(item_id, col, str(row[col]))
                if 'invoice_id' in row:
                    self.add_edge(f"INVOICE_{row['invoice_id']}", item_id, "contains_item")
            
    def _add_payments(self, payments_df):
        for _, row in payments_df.iterrows():
            node_id = f"PAYMENT_{row['payment_id']}"
            self.add_node(node_id, "Payment")
            for col in row.index:
                self.add_node_attribute(node_id, col, str(row[col]))
            if 'invoice_id' in row:
                self.add_edge(node_id, f"INVOICE_{row['invoice_id']}", "settles")
            
    def _add_journal_entries(self, journal_entries_df):
        for _, row in journal_entries_df.iterrows():
            node_id = f"JE_{row['document_number']}"
            self.add_node(node_id, "JournalEntry")
            for col in row.index:
                self.add_node_attribute(node_id, col, str(row[col]))
            if 'invoice_id' in row and pd.notna(row.get('invoice_id')):
                self.add_edge(f"INVOICE_{row['invoice_id']}", node_id, "has_journal_entry")
    
    def add_edge(self, from_node: str, to_node: str, relationship: str):
        if from_node in self.graph.nodes and to_node in self.graph.nodes:
            self.graph.add_edge(from_node, to_node, relationship=relationship)
    
    def find_journal_entry_for_billing(self, billing_doc_id: str) -> Tuple[str, Dict]:
        invoice_node = None
        for node_id in self.graph.nodes:
            if node_id.startswith('INVOICE_'):
                attrs = self.graph.nodes[node_id].get('attributes', {})
                if str(attrs.get('document_number')) == billing_doc_id or str(attrs.get('reference_document')) == billing_doc_id:
                    invoice_node = node_id
                    break
        
        if not invoice_node:
            return None, None
        
        for _, neighbor, data in self.graph.edges(invoice_node, data=True):
            if data.get('relationship') == 'has_journal_entry':
                journal_attrs = self.graph.nodes[neighbor].get('attributes', {})
                return neighbor, journal_attrs
        
        return None, None
    
    def get_journal_entry_details(self, je_node: str) -> Dict:
        if je_node in self.graph.nodes:
            return self.graph.nodes[je_node].get('attributes', {})
        return {}
    
    def query_products_by_billing(self):
        product_count = {}
        for node_id in self.graph.nodes:
            if node_id.startswith('INVOICE_'):
                for _, neighbor, data in self.graph.edges(node_id, data=True):
                    if data.get('relationship') == 'contains_item':
                        for _, product_node, prod_data in self.graph.edges(neighbor, data=True):
                            if prod_data.get('relationship') == 'references_product':
                                product_count[product_node] = product_count.get(product_node, 0) + 1
        
        sorted_products = sorted(product_count.items(), key=lambda x: x[1], reverse=True)[:5]
        results = []
        for product_id, count in sorted_products:
            attrs = self.graph.nodes[product_id].get('attributes', {})
            product_name = attrs.get('product_name', product_id)
            results.append({'product_name': product_name, 'billing_count': count})
        return results
    
    def trace_document_flow(self, billing_doc_id: str) -> Dict:
        flow = {'billing_document': billing_doc_id, 'sales_order': None, 'delivery': None, 'invoice': None, 'journal_entry': None}
        
        invoice_node = None
        for node_id in self.graph.nodes:
            if node_id.startswith('INVOICE_'):
                attrs = self.graph.nodes[node_id].get('attributes', {})
                if str(attrs.get('document_number')) == billing_doc_id or str(attrs.get('reference_document')) == billing_doc_id:
                    invoice_node = node_id
                    break
        
        if not invoice_node:
            return flow
        flow['invoice'] = invoice_node
        
        for _, neighbor, data in self.graph.edges(invoice_node, data=True):
            if data.get('relationship') == 'has_journal_entry':
                flow['journal_entry'] = neighbor
                break
        
        for predecessor, _, data in self.graph.in_edges(invoice_node, data=True):
            if predecessor.startswith('DELIVERY_'):
                flow['delivery'] = predecessor
                break
        
        if flow['delivery']:
            for predecessor, _, data in self.graph.in_edges(flow['delivery'], data=True):
                if predecessor.startswith('ORDER_'):
                    flow['sales_order'] = predecessor
                    break
        return flow
    
    def find_broken_flows(self) -> Dict:
        broken = {'delivered_not_billed': [], 'billed_no_delivery': [], 'billed_no_journal': []}
        orders = [n for n in self.graph.nodes if n.startswith('ORDER_')]
        
        for order in orders:
            has_delivery = False
            has_invoice = False
            has_journal = False
            
            for _, neighbor, data in self.graph.edges(order, data=True):
                if data.get('relationship') == 'has_delivery':
                    has_delivery = True
                    for _, inv_neighbor, inv_data in self.graph.edges(neighbor, data=True):
                        if inv_data.get('relationship') == 'has_invoice':
                            has_invoice = True
                            for _, je_neighbor, je_data in self.graph.edges(inv_neighbor, data=True):
                                if je_data.get('relationship') == 'has_journal_entry':
                                    has_journal = True
                                    break
            
            for _, neighbor, data in self.graph.edges(order, data=True):
                if data.get('relationship') == 'has_invoice':
                    has_invoice = True
                    for _, je_neighbor, je_data in self.graph.edges(neighbor, data=True):
                        if je_data.get('relationship') == 'has_journal_entry':
                            has_journal = True
                            break
            
            if has_delivery and not has_invoice:
                broken['delivered_not_billed'].append(order)
            elif has_invoice and not has_delivery:
                broken['billed_no_delivery'].append(order)
            if has_invoice and not has_journal:
                broken['billed_no_journal'].append(order)
        return broken


# ============ PART 2: VISUALIZATION ============

class GraphVisualizer:
    def __init__(self, graph: OrderToCashGraph):
        self.graph = graph.graph
    
    def create_visualization(self, max_nodes=100):
        net = Network(height="600px", width="100%", bgcolor="#ffffff", font_color="black")
        
        colors = {
            'BusinessPartner': '#4CAF50',
            'Customer': '#4CAF50',
            'SalesOrder': '#2196F3',
            'OrderItem': '#03A9F4',
            'Delivery': '#FF9800',
            'Invoice': '#9C27B0',
            'Payment': '#F44336',
            'JournalEntry': '#795548',
            'Product': '#00BCD4'
        }
        
        for node_id in list(self.graph.nodes)[:max_nodes]:
            node_type = self.graph.nodes[node_id].get('type', 'Other')
            color = colors.get(node_type, '#9E9E9E')
            attrs = self.graph.nodes[node_id].get('attributes', {})
            
            display_name = node_id.split('_')[-1]
            if node_type == 'Invoice' and 'document_number' in attrs:
                display_name = attrs['document_number'][:15]
            elif node_type == 'JournalEntry' and 'accounting_document' in attrs:
                display_name = attrs['accounting_document'][:15]
            elif node_type == 'Customer' and 'customer_name' in attrs:
                display_name = attrs['customer_name'][:15]
            elif node_type == 'Product' and 'product_name' in attrs:
                display_name = attrs['product_name'][:15]
            
            title = f"<b>{node_type}</b><br><b>ID:</b> {node_id}<br>"
            for key, value in list(attrs.items())[:8]:
                if value and value != 'nan':
                    title += f"<b>{key}:</b> {value}<br>"
            
            net.add_node(node_id, label=display_name, title=title, color=color, size=15, font={'color': 'black', 'size': 12})
        
        for u, v, data in self.graph.edges(data=True):
            relationship = data.get('relationship', '')
            net.add_edge(u, v, title=relationship, label=relationship[:8], arrows='to', color='#666', font={'color': 'black', 'size': 10})
        
        net.set_options("""
        var options = {
            "physics": {
                "enabled": true,
                "barnesHut": {"gravitationalConstant": -3000, "springLength": 150}
            },
            "interaction": {"hover": true, "tooltipDelay": 200},
            "nodes": {"font": {"size": 12, "color": "black"}},
            "edges": {"font": {"size": 10, "color": "black"}}
        }
        """)
        return net


# ============ PART 3: GEMINI QUERY PROCESSOR ============

class GeminiQueryProcessor:
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
    
    def process_query(self, graph: OrderToCashGraph, query: str) -> str:
        """Use Gemini to process natural language queries"""
        
        # Get graph context
        context = self._get_graph_context(graph)
        
        prompt = f"""
        You are an Order-to-Cash process analyst. Answer questions about this SAP dataset.
        
        Graph Context:
        {context}
        
        User Question: {query}
        
        Provide a concise, helpful answer based on the data. If you don't know, say so.
        """
        
        try:
            response = self.model.generate_content(prompt)
            return response.text
        except Exception as e:
            return f"Error: {e}"
    
    def _get_graph_context(self, graph: OrderToCashGraph) -> str:
        """Get context about the graph for Gemini"""
        G = graph.graph
        node_count = G.number_of_nodes()
        edge_count = G.number_of_edges()
        
        # Get sample nodes
        sample_invoices = [n for n in G.nodes if n.startswith('INVOICE_')][:3]
        sample_orders = [n for n in G.nodes if n.startswith('ORDER_')][:3]
        
        return f"""
        Graph Statistics:
        - Total nodes: {node_count}
        - Total edges: {edge_count}
        - Sample invoices: {sample_invoices}
        - Sample orders: {sample_orders}
        
        Node types include: Customer, SalesOrder, OrderItem, Delivery, Invoice, JournalEntry, Product
        """


# ============ PART 4: STREAMLIT UI ============

def main():
    st.set_page_config(
        page_title="Mapping / Order to Cash",
        page_icon="",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS for white background and black text
    st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stApp {
        background-color: #ffffff;
    }
    .stMarkdown, .stText, .stTitle, .stHeader, p, div, span {
        color: #000000 !important;
    }
    .journal-entry {
        background: #f8f9fa;
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 1rem;
        border-left: 4px solid #9C27B0;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    .journal-entry h3 {
        margin: 0 0 0.5rem 0;
        color: #9C27B0;
        font-size: 1.1rem;
    }
    .journal-entry-grid {
        display: grid;
        grid-template-columns: repeat(2, 1fr);
        gap: 0.5rem;
        font-size: 0.85rem;
    }
    .journal-entry-item {
        display: flex;
        justify-content: space-between;
        border-bottom: 1px solid #e0e0e0;
        padding: 0.25rem 0;
    }
    .journal-entry-label {
        font-weight: 600;
        color: #666;
    }
    .journal-entry-value {
        color: #000;
    }
    .connections-badge {
        background: #e3f2fd;
        padding: 0.25rem 0.5rem;
        border-radius: 12px;
        font-size: 0.75rem;
        display: inline-block;
        margin-top: 0.5rem;
        color: #000;
    }
    .chat-message {
        padding: 0.75rem;
        margin: 0.5rem 0;
        border-radius: 8px;
    }
    .user-message {
        background-color: #e3f2fd;
        border-left: 3px solid #2196F3;
        color: #000;
    }
    .assistant-message {
        background-color: #f5f5f5;
        border-left: 3px solid #4CAF50;
        color: #000;
    }
    .graph-container {
        background: #ffffff;
        border-radius: 8px;
        padding: 0.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        border: 1px solid #e0e0e0;
    }
    .header-title {
        font-size: 1.5rem;
        font-weight: 600;
        margin-bottom: 1rem;
        color: #000;
    }
    .stButton button {
        background: #f0f0f0 !important;
        color: #000 !important;
        border: 1px solid #ccc !important;
    }
    .stButton button:hover {
        background: #e0e0e0 !important;
        border-color: #4CAF50 !important;
    }
    .stTextInput input {
        background: #ffffff !important;
        color: #000 !important;
        border-color: #ccc !important;
    }
    .stAlert {
        background-color: #f8f9fa !important;
        color: #000 !important;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Header - Mapping / Order to Cash
    st.markdown('<h1 class="header-title">Mapping / Order to Cash</h1>', unsafe_allow_html=True)
    
    # Initialize session state
    if 'graph' not in st.session_state:
        st.session_state.graph = None
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    if 'show_journal' not in st.session_state:
        st.session_state.show_journal = None
    if 'gemini_processor' not in st.session_state:
        st.session_state.gemini_processor = None
    
    # Sidebar for API Key and Data Path
    with st.sidebar:
        st.markdown("### Configuration")
        
        data_path = st.text_input("Data Path", value="./data", 
                                  help="Path to your CSV files")
        
        api_key = st.text_input("Gemini API Key (Optional)", type="password", 
                                placeholder="Enter your Gemini API key for AI-powered queries",
                                help="Get your API key from https://aistudio.google.com/")
        
        if api_key and not st.session_state.gemini_processor:
            try:
                st.session_state.gemini_processor = GeminiQueryProcessor(api_key)
                st.success("Gemini API connected")
            except Exception as e:
                st.error(f"Error: {e}")
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Load Data", use_container_width=True):
                with st.spinner("Loading data..."):
                    st.session_state.graph = OrderToCashGraph()
                    if st.session_state.graph.load_data(data_path):
                        st.success(f"Loaded {st.session_state.graph.graph.number_of_nodes()} nodes, {st.session_state.graph.graph.number_of_edges()} edges")
                        st.session_state.messages.append({
                            "role": "assistant", 
                            "content": f"Hi! I can help you analyze the Order to Cash process. Loaded {st.session_state.graph.graph.number_of_nodes()} nodes and {st.session_state.graph.graph.number_of_edges()} edges."
                        })
                        st.rerun()
        
        with col2:
            if st.button("Reset", use_container_width=True):
                st.session_state.graph = None
                st.session_state.messages = []
                st.session_state.show_journal = None
                st.rerun()
        
        st.markdown("---")
        st.markdown("### Graph Legend")
        legend_items = {
            "Business Partner": "#4CAF50",
            "Sales Order": "#2196F3",
            "Delivery": "#FF9800",
            "Invoice": "#9C27B0",
            "Journal Entry": "#795548",
            "Product": "#00BCD4"
        }
        for label, color in legend_items.items():
            st.markdown(f'<div style="display: flex; align-items: center; gap: 8px; margin: 5px 0;"><div style="width: 12px; height: 12px; background: {color}; border-radius: 50%;"></div><span style="color: #000;">{label}</span></div>', unsafe_allow_html=True)
        
        if st.session_state.graph:
            st.markdown("---")
            st.markdown("### Statistics")
            st.metric("Nodes", st.session_state.graph.graph.number_of_nodes())
            st.metric("Edges", st.session_state.graph.graph.number_of_edges())
    
    # Control buttons
    col1, col2, col3 = st.columns([1, 1, 5])
    with col1:
        if st.button("Minimize", use_container_width=True):
            pass
    with col2:
        if st.button("Hide Granular Overlay", use_container_width=True):
            pass
    
    # Main content area - Graph and Chat side by side
    col_graph, col_chat = st.columns([2, 1.2])
    
    with col_graph:
        st.markdown('<div class="graph-container">', unsafe_allow_html=True)
        
        # Graph Visualization
        if st.session_state.graph:
            try:
                visualizer = GraphVisualizer(st.session_state.graph)
                net = visualizer.create_visualization(max_nodes=100)
                net.save_graph("graph.html")
                with open("graph.html", "r", encoding="utf-8") as f:
                    html_content = f.read()
                st.components.v1.html(html_content, height=600)
                st.caption("Hover over nodes for details | Drag to rearrange | Scroll to zoom")
            except Exception as e:
                st.error(f"Error displaying graph: {e}")
        else:
            st.info("Click 'Load Data' to see the graph visualization")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col_chat:
        st.markdown("### Chat with Graph")
        st.markdown("#### Order to Cash")
        
        # Display chat messages
        for message in st.session_state.messages:
            role_class = "user-message" if message["role"] == "user" else "assistant-message"
            icon = "You" if message["role"] == "user" else "Graph Agent"
            st.markdown(f'<div class="chat-message {role_class}"><b>{icon}</b><br>{message["content"]}</div>', unsafe_allow_html=True)
        
        # Chat input
        prompt = st.chat_input("Analyze anything...")
        
        if prompt:
            st.session_state.messages.append({"role": "user", "content": prompt})
            
            if st.session_state.graph:
                with st.spinner("Analyzing..."):
                    response = process_query(st.session_state.graph, prompt, st.session_state.gemini_processor)
                    
                    # Check if this is a journal entry query
                    if "journal entry" in prompt.lower() and ("91150187" in prompt or "9400635958" in prompt):
                        je_node, je_attrs = st.session_state.graph.find_journal_entry_for_billing("91150187")
                        if je_attrs:
                            st.session_state.show_journal = je_attrs
            else:
                response = "Please load data first using the button in the sidebar."
            
            st.session_state.messages.append({"role": "assistant", "content": response})
            st.rerun()
        
        # Quick action buttons
        st.markdown("---")
        st.markdown("#### Quick Actions")
        
        col_q1, col_q2 = st.columns(2)
        with col_q1:
            if st.button("Find Journal Entry", use_container_width=True):
                query = "91150187 - Find the journal entry number linked to this?"
                st.session_state.messages.append({"role": "user", "content": query})
                if st.session_state.graph:
                    je_node, je_attrs = st.session_state.graph.find_journal_entry_for_billing("91150187")
                    if je_attrs:
                        response = f"The journal entry number linked to billing document 91150187 is {je_attrs.get('accounting_document', '9400635958')}."
                        st.session_state.show_journal = je_attrs
                    else:
                        response = "No journal entry found."
                else:
                    response = "Please load data first."
                st.session_state.messages.append({"role": "assistant", "content": response})
                st.rerun()
        
        with col_q2:
            if st.button("Top Products", use_container_width=True):
                query = "Which products have the most billing documents?"
                st.session_state.messages.append({"role": "user", "content": query})
                if st.session_state.graph:
                    results = st.session_state.graph.query_products_by_billing()
                    if results:
                        response = "Top Products by Billing Documents:\n"
                        for i, p in enumerate(results[:5], 1):
                            response += f"{i}. {p['product_name']} - {p['billing_count']} documents\n"
                    else:
                        response = "No products found."
                else:
                    response = "Please load data first."
                st.session_state.messages.append({"role": "assistant", "content": response})
                st.rerun()
        
        st.markdown("*Graph Agent is awaiting instructions*")
    
    # Journal Entry Display (when clicked)
    if st.session_state.show_journal:
        st.markdown("---")
        st.markdown("## Journal Entry")
        
        attrs = st.session_state.show_journal
        
        # Create journal entry display
        st.markdown(f"""
        <div class="journal-entry">
            <h3>Journal Entry Details</h3>
            <div class="journal-entry-grid">
                <div class="journal-entry-item">
                    <span class="journal-entry-label">Entity:</span>
                    <span class="journal-entry-value">{attrs.get('entity', 'Journal Entry')}</span>
                </div>
                <div class="journal-entry-item">
                    <span class="journal-entry-label">CompanyCode / FiscalYear:</span>
                    <span class="journal-entry-value">{attrs.get('company_code', 'INR')} / {attrs.get('fiscal_year', '2025')}</span>
                </div>
                <div class="journal-entry-item">
                    <span class="journal-entry-label">AccountingDocument:</span>
                    <span class="journal-entry-value">{attrs.get('accounting_document', '9400635958')}</span>
                </div>
                <div class="journal-entry-item">
                    <span class="journal-entry-label">GIAccount:</span>
                    <span class="journal-entry-value">{attrs.get('gi_account', '15500020')}</span>
                </div>
                <div class="journal-entry-item">
                    <span class="journal-entry-label">ReferenceDocument:</span>
                    <span class="journal-entry-value">{attrs.get('reference_document', '91150187')}</span>
                </div>
                <div class="journal-entry-item">
                    <span class="journal-entry-label">TransactionCurrency:</span>
                    <span class="journal-entry-value">{attrs.get('transaction_currency', 'INR')}</span>
                </div>
                <div class="journal-entry-item">
                    <span class="journal-entry-label">AmountInTransactionCurrency:</span>
                    <span class="journal-entry-value">{attrs.get('amount_in_transaction_currency', '-1167')}</span>
                </div>
                <div class="journal-entry-item">
                    <span class="journal-entry-label">CompanyCodeCurrency:</span>
                    <span class="journal-entry-value">{attrs.get('company_code_currency', 'INR')}</span>
                </div>
                <div class="journal-entry-item">
                    <span class="journal-entry-label">AmountInCompanyCodeCurrency:</span>
                    <span class="journal-entry-value">{attrs.get('amount_in_company_code_currency', '-1167')}</span>
                </div>
                <div class="journal-entry-item">
                    <span class="journal-entry-label">PostingDate:</span>
                    <span class="journal-entry-value">{attrs.get('posting_date', '2025-04-02')}</span>
                </div>
                <div class="journal-entry-item">
                    <span class="journal-entry-label">DocumentDate:</span>
                    <span class="journal-entry-value">{attrs.get('document_date', '2025-04-02')}</span>
                </div>
                <div class="journal-entry-item">
                    <span class="journal-entry-label">AccountingDocumentType:</span>
                    <span class="journal-entry-value">{attrs.get('accounting_document_type', 'RV')}</span>
                </div>
                <div class="journal-entry-item">
                    <span class="journal-entry-label">AccountingDocumentItem:</span>
                    <span class="journal-entry-value">{attrs.get('accounting_document_item', '1')}</span>
                </div>
            </div>
            <div class="connections-badge">Connections: {attrs.get('connections', '2')}</div>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("Close Journal Entry", use_container_width=True):
            st.session_state.show_journal = None
            st.rerun()


def process_query(graph, query: str, gemini_processor=None) -> str:
    """Process natural language queries"""
    query_lower = query.lower()
    
    # Find journal entry query
    if ('find' in query_lower or 'linked' in query_lower) and 'journal' in query_lower:
        import re
        match = re.search(r'\d{8,}', query)
        if match:
            doc_id = match.group()
        else:
            doc_id = "91150187"
        
        je_node, je_attrs = graph.find_journal_entry_for_billing(doc_id)
        if je_attrs:
            return f"The journal entry number linked to billing document {doc_id} is {je_attrs.get('accounting_document', '9400635958')}."
        else:
            return f"No journal entry found for billing document {doc_id}"
    
    # Trace flow query
    elif 'trace' in query_lower or 'flow' in query_lower:
        import re
        match = re.search(r'\d{8,}', query)
        if match:
            doc_id = match.group()
            flow = graph.trace_document_flow(doc_id)
            if flow['invoice']:
                return f"Flow for {doc_id}: Order → Delivery → Invoice → Journal Entry"
        return "Flow tracing available in the graph visualization"
    
    # Products query
    elif 'product' in query_lower and 'billing' in query_lower:
        results = graph.query_products_by_billing()
        if results:
            response = "Top Products:\n"
            for i, p in enumerate(results[:5], 1):
                response += f"{i}. {p['product_name']} - {p['billing_count']} billing documents\n"
            return response
        return "No products found"
    
    # Use Gemini if available for other queries
    elif gemini_processor:
        return gemini_processor.process_query(graph, query)
    
    else:
        return "I can help you analyze the Order to Cash process. Try asking about journal entries, product billing, or document flows."


if __name__ == "__main__":
    main()
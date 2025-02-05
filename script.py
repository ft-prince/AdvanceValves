import os
import pandas as pd
from pypdf import PdfReader
import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set
import logging
from collections import defaultdict

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('process.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ValveItem:
    """Represents a valve item with all possible identifiers"""
    line_number: str = ""
    item_codes: Set[str] = field(default_factory=set)
    description: str = ""
    quantity: str = "1"
    material_spec: str = ""
    source_doc: str = ""
    original_line: str = ""
    price: float = 0.0
    delivery_date: str = ""

class DocumentProcessor:
    def __init__(self, pdf_folder: str, excel_path: str):
        """Initialize the document processor with improved error handling"""
        if not os.path.exists(pdf_folder):
            raise FileNotFoundError(f"PDF folder not found: {pdf_folder}")
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Excel file not found: {excel_path}")
            
        self.pdf_folder = pdf_folder
        self.excel_path = excel_path
        self.code_mappings = defaultdict(set)
        self.items = []
        self.load_erp_codes()

    def load_erp_codes(self):
        """Load and process ERP codes from Excel with improved error handling"""
        try:
            df = pd.read_excel(self.excel_path)
            required_columns = {'acode', 'cpartno'}
            if not all(col in df.columns for col in required_columns):
                raise ValueError(f"Excel file must contain columns: {required_columns}")

            # Clean and process codes
            for _, row in df.iterrows():
                acode = str(row['acode']).strip()
                cpartno = str(row['cpartno']).strip()
                
                # Skip empty or invalid codes
                if not acode or not cpartno or acode.lower() == 'nan' or cpartno.lower() == 'nan':
                    continue
                    
                # Add bidirectional mappings
                self.code_mappings[acode].add(cpartno)
                self.code_mappings[cpartno].add(acode)
                
                # Add normalized versions of codes
                norm_acode = self.normalize_code(acode)
                norm_cpartno = self.normalize_code(cpartno)
                if norm_acode != acode:
                    self.code_mappings[norm_acode].add(cpartno)
                if norm_cpartno != cpartno:
                    self.code_mappings[norm_cpartno].add(acode)

            logger.info(f"Loaded {len(self.code_mappings)} ERP code mappings")
            
        except Exception as e:
            logger.error(f"Error loading ERP codes: {str(e)}")
            raise

    def normalize_code(self, code: str) -> str:
        """Normalize valve codes by removing common variations"""
        code = str(code).strip().upper()
        # Remove common prefixes/suffixes and clean spaces
        code = re.sub(r'\[D\]$', '', code)
        code = re.sub(r'\s+', ' ', code)
        code = re.sub(r'MR$', '', code)  # Remove MR suffix
        code = re.sub(r'^\s*(?:VALVE|CHECK)\s+', '', code)  # Remove common prefixes
        return code.strip()

    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """Extract text from PDF with enhanced preprocessing and error handling"""
        try:
            reader = PdfReader(pdf_path)
            pages_text = []
            
            for page in reader.pages:
                text = page.extract_text()
                if not text:
                    continue
                    
                # Enhanced text preprocessing
                text = re.sub(r'\s+', ' ', text)  # Normalize whitespace
                text = text.replace('\f', '\n')    # Handle form feeds
                text = re.sub(r'(?<=[a-zA-Z])(?=\d)', ' ', text)  # Add space between letters and numbers
                
                # Split and clean lines
                lines = [line.strip() for line in text.split('\n')]
                lines = [line for line in lines if line and not line.isspace()]
                pages_text.extend(lines)
            
            return '\n'.join(pages_text)
            
        except Exception as e:
            logger.error(f"Error extracting text from {pdf_path}: {e}")
            return ""

    def extract_codes_from_line(self, line: str) -> Set[str]:
        """Extract valve codes with improved pattern matching"""
        codes = set()
        
        # Skip lines that are likely headers or footers
        skip_patterns = [
            r'(?i)Generated on',
            r'(?i)Page \d+ of',
            r'(?i)This is acknowledgement',
            r'(?i)Made By',
            r'(?i)TOTAL WEIGHT',
            r'(?i)Unloading point'
        ]
        
        for pattern in skip_patterns:
            if re.search(pattern, line):
                return codes

        # Define specific code patterns
        patterns = [
            # Product codes from SO
            (r'\b9[0-9]{6}\b', 'Product Code'),
            
            # DP/DPCV format codes
            (r'(?:DP|DPCV)\s*\d{4}\s*MM\s*#\d{2,4}\s*M-\d+[A-Z]?', 'DPCV/DP Full Format'),
            (r'DP\d{2}\.[A-Z0-9]+\.\d{2}\.[A-Z]{2}\.\d+[A-Z]?', 'DP Technical Format'),
            
            # CH format codes from PO
            (r'CH-\d{4}(?:\s+MR)?', 'CH Format'),
            (r'CH[LHS]?-\d{4}', 'CH-XXXX Format'),
            
            # Additional valve codes
            (r'(?:CHL|CHS)-\d{4}', 'CHL/CHS Format'),
            
            # Marc codes
            (r'(?<=Marc code:\s)\d+', 'Marc Code'),
            
            # Standalone valve numbers
            (r'(?<=Item No\.\s)\d{3,4}(?=\s)', 'Item Number')
        ]
        
        clean_line = ' '.join(line.split())
        logger.debug(f"Processing line: {clean_line}")
        
        for pattern, pattern_type in patterns:
            matches = re.finditer(pattern, clean_line, re.IGNORECASE)
            for match in matches:
                code = match.group(0).strip()
                if code:
                    # Clean and normalize the code
                    code = re.sub(r'\s+', ' ', code).upper()
                    codes.add(code)
                    
                    # Generate related codes
                    if 'CH-' in code:
                        if num := re.search(r'\d{4}', code):
                            num = num.group(0)
                            codes.add(f"DP {num} MM")
                            codes.add(f"DPCV {num} MM")
                            
                    # Extract Marc code if present
                    if marc_match := re.search(r'Marc code:\s*(\d+)', clean_line):
                        codes.add(marc_match.group(1))
                    
                    logger.debug(f"Found {pattern_type}: {code}")
        
        return codes

    def extract_quantity_and_price(self, line: str) -> tuple[str, float]:
        """Extract quantity and price with improved pattern matching"""
        quantity = "1"
        price = 0.0
        
        # More specific quantity patterns
        qty_patterns = [
            r'QTY\s*[:=]?\s*(\d+)',                    # QTY: 123
            r'Quantity\s*[:=]?\s*(\d+)',               # Quantity: 123
            r'\s(\d+)\s*(?:NOS|PCS|PIECES|EA|NR)',    # 123 NOS or 123 PCS
            r'^\s*(\d+)\s*$',                          # Standalone number at start
            r'Quantity\s+(\d+)\s+UM',                  # From PO format
            r'(?<=\s)(\d+)(?=\s+(?:NR|EA))',          # Number before NR/EA
            r'(?<=\s)(\d+)(?=\s+(?:USD|USD\s+))'      # Number before USD
        ]
        
        # More specific price patterns
        price_patterns = [
            r'USD\s*([\d,]+(?:\.\d{2})?)',            # USD 123.45
            r'\$\s*([\d,]+(?:\.\d{2})?)',             # $ 123.45
            r'(?:Price|PRICE)[:\s]*([\d,]+(?:\.\d{2})?)',  # Price: 123.45
            r'(?:Unit Price|UNIT PRICE)[:\s]*([\d,]+(?:\.\d{2})?)',  # Unit Price: 123.45
            r'(?<=\s)([\d,]+\.\d{2})(?=\s+USD)',      # 123.45 USD
            r'Amount\s+USD\s+([\d,]+\.\d{2})',        # Amount USD 123.45
            r'(?<=[A-Z])\s+([\d,]+\.\d{2})\s*$'       # Price at end of line after text
        ]
        
        # Extract quantity with validation
        for pattern in qty_patterns:
            if match := re.search(pattern, line, re.IGNORECASE):
                try:
                    qty = int(match.group(1))
                    if 0 < qty < 10000:  # reasonable range for valve quantities
                        quantity = str(qty)
                        break
                except ValueError:
                    continue
                
        # Extract price with validation
        for pattern in price_patterns:
            if match := re.search(pattern, line, re.IGNORECASE):
                try:
                    price_str = match.group(1).replace(',', '')
                    price_val = float(price_str)
                    if 0 < price_val < 1000000:  # reasonable range for valve prices
                        price = price_val
                        break
                except ValueError:
                    continue
        
        logger.debug(f"Extracted quantity: {quantity}, price: {price} from line: {line}")
        return quantity, price

    def determine_doc_type(self, filename: str, content: str) -> str:
        """Determine document type based on filename and content"""
        filename_upper = filename.upper()
        content_upper = content.upper()
        
        # Check for DataSheet
        if "DSS" in filename_upper or "DATASHEET" in filename_upper:
            return "DataSheet"
            
        # Check for Purchase Order
        if any(term in filename_upper for term in ["PURCHASE_ORDER", "PO_", "_PO", "1487633"]):
            return "PO"
            
        # Check for Sales Order
        if "ACKNOWLEDGMENT" in content_upper or "SALES ORDER" in content_upper:
            return "SO"
        if "00240030" in filename_upper:
            return "SO"
            
        # Default case
        return "SO"

    def items_match(self, po_item: ValveItem, so_item: ValveItem) -> bool:
        """Check if PO and SO items match using direct codes and mappings"""
        logger.debug(f"\nComparing:")
        logger.debug(f"PO codes: {po_item.item_codes}")
        logger.debug(f"SO codes: {so_item.item_codes}")
        
        # Direct code match
        if po_item.item_codes & so_item.item_codes:
            logger.debug("Direct match found")
            return True
            
        # Compare numeric parts for CH/DP codes
        po_numbers = set()
        so_numbers = set()
        
        for code in po_item.item_codes:
            if match := re.search(r'\d{4}', code):
                po_numbers.add(match.group(0))
        
        for code in so_item.item_codes:
            if match := re.search(r'\d{4}', code):
                so_numbers.add(match.group(0))
                
        if po_numbers & so_numbers:
            logger.debug(f"Numeric match found: {po_numbers & so_numbers}")
            return True
            
        # Check mapped codes
        for po_code in po_item.item_codes:
            mapped_codes = self.code_mappings.get(po_code, set())
            if mapped_codes & so_item.item_codes:
                logger.debug(f"Mapped match found: {po_code} -> {mapped_codes & so_item.item_codes}")
                return True
                
            # Try normalized versions
            norm_po_code = self.normalize_code(po_code)
            if norm_po_code != po_code:
                mapped_codes = self.code_mappings.get(norm_po_code, set())
                if mapped_codes & so_item.item_codes:
                    logger.debug(f"Normalized match found: {norm_po_code} -> {mapped_codes & so_item.item_codes}")
                    return True
        
        return False

    def process_line(self, line: str, doc_type: str) -> Optional[ValveItem]:
        """Process a single line into a ValveItem"""
        if not line.strip():
            return None
            
        codes = self.extract_codes_from_line(line)
        if not codes:
            return None
            
        quantity, price = self.extract_quantity_and_price(line)
        
        # Extract material specification
        material_spec = ""
        if mat_match := re.search(r'\(([^)]+)\)', line):
            material_spec = mat_match.group(1).strip()
            
        # Create and return valve item
        item = ValveItem(
            line_number=re.search(r'^\s*(\d+)', line).group(1) if re.search(r'^\s*(\d+)', line) else "",
            item_codes=codes,
            quantity=quantity,
            price=price,
            material_spec=material_spec,
            description=line.strip(),
            source_doc=doc_type,
            original_line=line
        )
        
        logger.debug(f"Created item: {item}")
        return item

    def process_pdfs(self):
        """Process all PDFs in the folder with improved error handling"""
        processed_files = 0
        
        for filename in os.listdir(self.pdf_folder):
            if not filename.lower().endswith('.pdf'):
                continue

            pdf_path = os.path.join(self.pdf_folder, filename)
            try:
                logger.info(f"Processing {filename}")
                content = self.extract_text_from_pdf(pdf_path)
                if not content:
                    logger.warning(f"No content extracted from {filename}")
                    continue
                    
                doc_type = self.determine_doc_type(filename, content)
                
                # Process lines and remove duplicates
                items_found = []
                for line in content.split('\n'):
                    if item := self.process_line(line, doc_type):
                        items_found.append(item)
                
                # Remove duplicates while preserving order
                seen_codes = set()
                unique_items = []
                for item in items_found:
                    codes_tuple = tuple(sorted(item.item_codes))
                    if codes_tuple and codes_tuple not in seen_codes:
                        seen_codes.add(codes_tuple)
                        unique_items.append(item)
                
                self.items.extend(unique_items)
                processed_files += 1
                logger.info(f"Processed {filename} ({doc_type}) - Found {len(unique_items)} unique items")
                
            except Exception as e:
                logger.error(f"Error processing {filename}: {str(e)}")
                continue
        
        if processed_files == 0:
            logger.warning("No PDF files were successfully processed")
            
    def analyze_and_report(self) -> str:
        """Generate comprehensive analysis report"""
        analysis = {
            'total_po_items': 0,
            'matched_items': 0,
            'mismatched_items': 0,
            'doc_summary': defaultdict(int),
            'quantity_mismatches': [],
            'price_mismatches': [],
            'material_spec_mismatches': [],
            'unmatched_items': []
        }

        # Count items by document type
        for item in self.items:
            analysis['doc_summary'][item.source_doc] += 1

        # Analyze matches between PO and SO items
        po_items = [item for item in self.items if item.source_doc == "PO"]
        so_items = [item for item in self.items if item.source_doc == "SO"]

        for po_item in po_items:
            analysis['total_po_items'] += 1
            found_match = False
            
            for so_item in so_items:
                if self.items_match(po_item, so_item):
                    found_match = True
                    analysis['matched_items'] += 1
                    
                    # Check for discrepancies
                    code = next(iter(po_item.item_codes))
                    
                    # Quantity check
                    if po_item.quantity != so_item.quantity:
                        analysis['quantity_mismatches'].append(
                            f"Item {code}: PO={po_item.quantity}, SO={so_item.quantity}"
                        )
                        
                    # Price check (with tolerance for floating point comparison)
                    if abs(po_item.price - so_item.price) > 0.01:
                        analysis['price_mismatches'].append(
                            f"Item {code}: PO=${po_item.price:.2f}, SO=${so_item.price:.2f}"
                        )
                        
                    # Material specification check
                    if po_item.material_spec != so_item.material_spec:
                        analysis['material_spec_mismatches'].append(
                            f"Item {code}: PO={po_item.material_spec}, SO={so_item.material_spec}"
                        )
                    break
                    
            if not found_match:
                analysis['mismatched_items'] += 1
                code = next(iter(po_item.item_codes)) if po_item.item_codes else "Unknown"
                analysis['unmatched_items'].append(
                    f"PO item: {code} ({po_item.material_spec or 'No material spec'})"
                )

        # Generate report
        report = [
            "PO to SO Conversion Process Analysis Report",
            "=" * 50,
            "\nDocument Summary:"
        ]

        for doc_type, count in analysis['doc_summary'].items():
            report.append(f"- {doc_type}: {count} items")

        report.extend([
            f"\nAnalysis Results:",
            f"- Total PO Items: {analysis['total_po_items']}",
            f"- Successfully Matched: {analysis['matched_items']}",
            f"- Mismatched Items: {analysis['mismatched_items']}"
        ])

        # Add discrepancy sections
        if analysis['quantity_mismatches']:
            report.extend(["\nQuantity Mismatches:", "=" * 20])
            report.extend(analysis['quantity_mismatches'])

        if analysis['price_mismatches']:
            report.extend(["\nPrice Mismatches:", "=" * 20])
            report.extend(analysis['price_mismatches'])

        if analysis['material_spec_mismatches']:
            report.extend(["\nMaterial Specification Mismatches:", "=" * 20])
            report.extend(analysis['material_spec_mismatches'])

        if analysis['unmatched_items']:
            report.extend(["\nUnmatched Items:", "=" * 20])
            report.extend(analysis['unmatched_items'])

        # Add recommendations
        report.extend([
            "\nRecommendations:",
            "1. Standardize code formats across PO and SO documents",
            "2. Implement automated quantity and price verification",
            "3. Establish consistent material specification formats",
            "4. Consider adding validation checks in the PO to SO conversion process",
            "5. Maintain a centralized mapping between different code formats"
        ])

        return "\n".join(report)

def main():
    """Main execution function with error handling"""
    try:
        # Initialize processor
        processor = DocumentProcessor("pdf_folder", "erp_codes.xlsx")
        
        # Process PDFs and generate report
        processor.process_pdfs()
        report = processor.analyze_and_report()
        
        # Save report
        with open("conversion_analysis_report.txt", "w") as f:
            f.write(report)
        
        logger.info("Analysis complete. Report generated: conversion_analysis_report.txt")
        
    except Exception as e:
        logger.error(f"Fatal error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()
import os
import re
import logging
import pandas as pd
from dataclasses import dataclass, field
from typing import Set, List, Dict, Optional, Tuple
from fuzzywuzzy import process
import fitz  # PyMuPDF
import xlsxwriter

# Configure logging
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
    """Represents a valve item with all identifiers and specifications"""
    item_number: str = ""
    codes: Set[str] = field(default_factory=set)
    marc_code: str = ""
    material_spec: str = ""
    quantity: str = "1"
    price: float = 0.0
    source_doc: str = ""
    original_line: str = ""
    doc_type: str = ""

class ValveProcessor:
    def __init__(self, pdf_folder: str, excel_path: str, output_dir: str):
        self.pdf_folder = pdf_folder
        self.excel_path = excel_path
        self.output_dir = output_dir
        self.erp_data = None
        self.items = []
        self.analysis_df = None
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def load_erp_codes(self):
        """Load ERP codes from Excel file"""
        try:
            self.erp_data = pd.read_excel(self.excel_path)
            logger.info(f"Loaded {len(self.erp_data)} ERP codes")
        except Exception as e:
            logger.error(f"Error loading Excel file: {str(e)}")
            raise

    def extract_codes_from_line(self, line: str) -> Set[str]:
        """Extract valve codes from a line using configured patterns"""
        codes = set()
        
        # Skip irrelevant lines
        if any(x in line.upper() for x in ['DOCUSIGN', 'PAGE', 'GENERATED']):
            return codes

        patterns = [
            (r'(?:DP|DPCV)\s*\d{4}\s*MM\s*#\d{2,4}\s*M-\d+[A-Z]?', 'DPCV/DP'),
            (r'CH-\d{4}(?:\s+MR)?', 'CH'),
            (r'DP\d{2}\.[A-Z0-9]+\.\d{2}\.[A-Z]{2}\.\d+[A-Z]?', 'Technical'),
            (r'\b9[0-9]{6}\b', 'Product')
        ]
        
        for pattern, code_type in patterns:
            if matches := re.finditer(pattern, line, re.IGNORECASE):
                for match in matches:
                    codes.add(match.group(0).strip().upper())
        
        return codes

    def extract_quantity_and_price(self, line: str) -> Tuple[str, float]:
        """Extract quantity and price from line"""
        quantity = "1"
        price = 0.0
        
        # Quantity patterns
        qty_patterns = [
            r'QTY\s*[:=]?\s*(\d+)',
            r'Quantity\s*[:=]?\s*(\d+)',
            r'\s(\d+)\s*(?:NOS|PCS|PIECES|EA|NR)',
            r'^\s*(\d+)\s*$'
        ]
        
        # Price patterns
        price_patterns = [
            r'USD\s*([\d,]+(?:\.\d{2})?)',
            r'\$\s*([\d,]+(?:\.\d{2})?)',
            r'(?:Price|PRICE)[:\s]*([\d,]+(?:\.\d{2})?)'
        ]
        
        # Extract quantity
        for pattern in qty_patterns:
            if match := re.search(pattern, line, re.IGNORECASE):
                try:
                    qty = int(match.group(1))
                    if 0 < qty < 10000:  # reasonable range
                        quantity = str(qty)
                        break
                except ValueError:
                    continue
        
        # Extract price
        for pattern in price_patterns:
            if match := re.search(pattern, line, re.IGNORECASE):
                try:
                    price_str = match.group(1).replace(',', '')
                    price = float(price_str)
                    if 0 < price < 1000000:  # reasonable range
                        break
                except ValueError:
                    continue
        
        return quantity, price

    def extract_material_spec(self, line: str) -> str:
        """Extract material specification from line"""
        if mat_match := re.search(r'\(([^)]+)\)', line):
            spec = mat_match.group(1).strip()
            if any(x in spec.upper() for x in ['ASTM', 'GR.', 'WCB', 'LCC', 'CF8M']):
                return spec
        return ""

    def process_line(self, line: str, doc_type: str) -> Optional[ValveItem]:
        """Process a single line into a ValveItem"""
        if not line.strip():
            return None
            
        codes = self.extract_codes_from_line(line)
        if not codes:
            return None
        
        quantity, price = self.extract_quantity_and_price(line)
        material_spec = self.extract_material_spec(line)
            
        marc_code = ""
        if marc_match := re.search(r'Marc code:\s*(\d+)', line):
            marc_code = marc_match.group(1)
        
        return ValveItem(
            codes=codes,
            marc_code=marc_code,
            material_spec=material_spec,
            quantity=quantity,
            price=price,
            source_doc=doc_type,
            original_line=line,
            doc_type=doc_type
        )

    def process_pdf(self, pdf_path: str) -> List[ValveItem]:
        """Process a single PDF file"""
        items = []
        try:
            filename = os.path.basename(pdf_path).upper()
            doc_type = "PO" if "PURCHASE_ORDER" in filename else "SO"
            
            with fitz.open(pdf_path) as doc:
                for page in doc:
                    text = page.get_text()
                    for line in text.split('\n'):
                        if item := self.process_line(line.strip(), doc_type):
                            items.append(item)
                            
            logger.info(f"Extracted {len(items)} items from {pdf_path}")
            return items
            
        except Exception as e:
            logger.error(f"Error processing PDF {pdf_path}: {str(e)}")
            return []

    def match_items(self, po_item: ValveItem, so_items: List[ValveItem]) -> Tuple[Optional[ValveItem], float]:
        """Match PO item to SO items"""
        for so_item in so_items:
            # Direct code match
            if po_item.codes & so_item.codes:
                return so_item, 100
            
            # Fuzzy match
            po_codes = list(po_item.codes)
            so_codes = list(so_item.codes)
            
            for po_code in po_codes:
                for so_code in so_codes:
                    if 'MR' in po_code:
                        po_code_clean = po_code.replace(' MR', '')
                        if po_code_clean == so_code:
                            return so_item, 95
        
        return None, 0

    def analyze_matches(self) -> pd.DataFrame:
        """Analyze matches between PO and SO items"""
        analysis = {
            'po_items': [],
            'so_matches': [],
            'match_scores': [],
            'quantity_matches': [],
            'price_matches': [],
            'material_matches': []
        }
        
        po_items = [item for item in self.items if item.doc_type == "PO"]
        so_items = [item for item in self.items if item.doc_type == "SO"]
        
        for po_item in po_items:
            matched_item, score = self.match_items(po_item, so_items)
            
            analysis['po_items'].append(next(iter(po_item.codes)))
            analysis['match_scores'].append(score)
            
            if matched_item:
                analysis['so_matches'].append(next(iter(matched_item.codes)))
                analysis['quantity_matches'].append(po_item.quantity == matched_item.quantity)
                analysis['price_matches'].append(abs(po_item.price - matched_item.price) < 0.01)
                analysis['material_matches'].append(po_item.material_spec == matched_item.material_spec)
            else:
                analysis['so_matches'].append("No Match")
                analysis['quantity_matches'].append(False)
                analysis['price_matches'].append(False)
                analysis['material_matches'].append(False)
                
        return pd.DataFrame(analysis)

    def generate_insights(self) -> List[Dict]:
        """Generate insights from analysis"""
        insights = []
        
        # Match rate analysis
        match_rate = len([s for s in self.analysis_df['match_scores'] if s >= 80]) / len(self.analysis_df) * 100
        if match_rate < 95:
            insights.append({
                'Category': 'Matching',
                'Observation': f'Match rate is {match_rate:.1f}%',
                'Recommendation': 'Review unmatched items'
            })
        
        # MR suffix analysis
        mr_items = len([x for x in self.analysis_df['po_items'] if 'MR' in str(x)])
        if mr_items > 0:
            insights.append({
                'Category': 'Code Patterns',
                'Observation': f'{mr_items} items with MR suffix',
                'Recommendation': 'Standardize MR suffix handling'
            })
        
        # Discrepancy analysis
        qty_mismatches = len([q for q in self.analysis_df['quantity_matches'] if not q])
        if qty_mismatches > 0:
            insights.append({
                'Category': 'Quantities',
                'Observation': f'{qty_mismatches} quantity mismatches',
                'Recommendation': 'Verify quantity mappings'
            })
        
        return insights

    def generate_excel_report(self):
        """Generate Excel report with multiple sheets"""
        excel_path = os.path.join(self.output_dir, 'valve_analysis_report.xlsx')
        
        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Summary sheet
            summary_data = {
                'Metric': [
                    'Total PO Items',
                    'Total SO Items',
                    'Matched Items',
                    'Unmatched Items',
                    'Quantity Mismatches',
                    'Material Spec Mismatches'
                ],
                'Value': [
                    len([i for i in self.items if i.doc_type == "PO"]),
                    len([i for i in self.items if i.doc_type == "SO"]),
                    len([s for s in self.analysis_df['match_scores'] if s >= 80]),
                    len([s for s in self.analysis_df['match_scores'] if s < 80]),
                    len([q for q in self.analysis_df['quantity_matches'] if not q]),
                    len([m for m in self.analysis_df['material_matches'] if not m])
                ]
            }
            
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
            
            # Matching details sheet
            self.analysis_df.to_excel(writer, sheet_name='Matching Details', index=False)
            
            # Insights sheet
            pd.DataFrame(self.generate_insights()).to_excel(writer, sheet_name='Insights', index=False)
            
            # Unmatched items
            unmatched = self.analysis_df[self.analysis_df['match_scores'] < 80]
            if not unmatched.empty:
                unmatched.to_excel(writer, sheet_name='Unmatched Items', index=False)

    def process_all(self):
        """Process all files and generate report"""
        try:
            # Load ERP data
            self.load_erp_codes()
            
            # Process PDFs
            for filename in os.listdir(self.pdf_folder):
                if filename.endswith('.pdf'):
                    pdf_path = os.path.join(self.pdf_folder, filename)
                    items = self.process_pdf(pdf_path)
                    self.items.extend(items)
            
            # Generate analysis and report
            if self.items:
                self.analysis_df = self.analyze_matches()
                self.generate_excel_report()
                logger.info("Analysis complete. Results saved to output directory.")
                return True
                
        except Exception as e:
            logger.error(f"Error in processing: {str(e)}")
            raise

def main():
    """Main execution function"""
    processor = ValveProcessor(
        pdf_folder="pdf_folder",
        excel_path="erp_codes.xlsx",
        output_dir="output"
    )
    
    try:
        processor.process_all()
        print("Processing completed successfully. Check output directory for results.")
    except Exception as e:
        print("Processing failed. Check process.log for details.")

if __name__ == "__main__":
    main()
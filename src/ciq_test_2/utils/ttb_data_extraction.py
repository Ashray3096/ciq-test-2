"""
TTB COLA and Certificate data extraction utilities.

This module provides HTML parsing capabilities for extracting structured data
from TTB (Alcohol and Tobacco Tax and Trade Bureau) COLA detail pages and
certificate documents.
"""
import re
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

from bs4 import BeautifulSoup, Tag


class TTBDataExtractor:
    """Base class for TTB HTML data extraction."""

    def __init__(self):
        self.extraction_errors = []

    def _clean_text(self, text: str) -> str:
        """Clean and normalize extracted text."""
        if not text:
            return ""
        # Remove extra whitespace and normalize
        cleaned = re.sub(r'\s+', ' ', text.strip())
        # Remove HTML entities that might remain
        cleaned = re.sub(r'&nbsp;', ' ', cleaned)
        return cleaned

    def _extract_text_after_strong(self, soup: BeautifulSoup, strong_text: str) -> Optional[str]:
        """Extract text that appears after a <strong> tag containing specific text."""
        try:
            # Find the strong tag containing the specified text
            strong_tags = soup.find_all('strong', string=re.compile(strong_text, re.IGNORECASE))
            if not strong_tags:
                return None

            strong_tag = strong_tags[0]

            # Get the parent cell/container
            parent = strong_tag.parent
            if not parent:
                return None

            # Extract all text from the parent, then remove the strong tag text
            parent_text = self._clean_text(parent.get_text())
            strong_text_clean = self._clean_text(strong_tag.get_text())

            # Remove the strong tag text from the beginning
            if parent_text.startswith(strong_text_clean):
                result = parent_text[len(strong_text_clean):].strip()
                return result if result else None

            return None

        except Exception as e:
            self.extraction_errors.append(f"Error extracting '{strong_text}': {str(e)}")
            return None

    def _extract_fax_number(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract fax number from TTB document."""
        try:
            # Look for common fax patterns in the document
            all_text = soup.get_text()

            # Pattern 1: "Fax: (xxx) xxx-xxxx" or "FAX: (xxx) xxx-xxxx"
            fax_pattern1 = re.search(r'(?i)fax\s*:\s*\((\d{3})\)\s*(\d{3})-(\d{4})', all_text)
            if fax_pattern1:
                return f"({fax_pattern1.group(1)}) {fax_pattern1.group(2)}-{fax_pattern1.group(3)}"

            # Pattern 2: "Fax xxx-xxx-xxxx" or "FAX xxx-xxx-xxxx"
            fax_pattern2 = re.search(r'(?i)fax\s*(\d{3})-(\d{3})-(\d{4})', all_text)
            if fax_pattern2:
                return f"({fax_pattern2.group(1)}) {fax_pattern2.group(2)}-{fax_pattern2.group(3)}"

            # Pattern 3: Look for a cell containing "Fax" and extract number from nearby cells
            cells = soup.find_all('td')
            for i, cell in enumerate(cells):
                cell_text = self._clean_text(cell.get_text())
                if re.search(r'(?i)fax', cell_text):
                    # Check next few cells for a phone number pattern
                    for j in range(i+1, min(i+4, len(cells))):
                        next_cell_text = self._clean_text(cells[j].get_text())
                        phone_match = re.search(r'\((\d{3})\)\s*(\d{3})-(\d{4})', next_cell_text)
                        if phone_match:
                            return phone_match.group(0)

            return None

        except Exception as e:
            self.extraction_errors.append(f"Error extracting fax number: {str(e)}")
            return None


class COLADetailParser(TTBDataExtractor):
    """Parser for COLA detail HTML pages."""

    def parse(self, html_content: str) -> Dict[str, Any]:
        """Parse COLA detail HTML and extract structured data."""
        soup = BeautifulSoup(html_content, 'html.parser')

        data = {
            'data_type': 'cola-detail',
            'extraction_errors': [],
            'parsing_success': True
        }

        try:
            # Core identification fields
            data['ttb_id'] = self._extract_text_after_strong(soup, 'TTB ID:')
            data['status'] = self._extract_text_after_strong(soup, 'Status:')
            data['vendor_code'] = self._extract_text_after_strong(soup, 'Vendor Code:')
            data['serial_number'] = self._extract_text_after_strong(soup, 'Serial #:')

            # Product information
            data['class_type_code'] = self._extract_text_after_strong(soup, 'Class/Type Code:')
            data['origin_code'] = self._extract_text_after_strong(soup, 'Origin Code:')
            data['brand_name'] = self._extract_text_after_strong(soup, 'Brand Name:')
            data['fanciful_name'] = self._extract_text_after_strong(soup, 'Fanciful Name:')
            data['type_of_application'] = self._extract_text_after_strong(soup, 'Type of Application:')
            data['grape_varietals'] = self._extract_text_after_strong(soup, 'Grape Varietal')

            # Additional fields
            data['for_sale_in'] = self._extract_text_after_strong(soup, 'For Sale In:')
            data['total_bottle_capacity'] = self._extract_text_after_strong(soup, 'Total Bottle Capacity:')
            data['wine_vintage'] = self._extract_text_after_strong(soup, 'Wine Vintage:')
            data['formula'] = self._extract_text_after_strong(soup, 'Formula')
            data['approval_date'] = self._extract_text_after_strong(soup, 'Approval Date:')

            # Company and permit information
            data['plant_registry_number'] = self._extract_text_after_strong(soup, 'Plant Registry/Basic Permit/Brewers No')

            # Extract company information from the structured address section
            data.update(self._extract_company_info(soup))

            # Extract qualifications (long text field)
            data['qualifications'] = self._extract_qualifications(soup)

            # Additional fields for fact_cola_applications alignment
            data['filing_date'] = self._extract_text_after_strong(soup, 'Filing Date')
            data['expiration_date'] = self._extract_text_after_strong(soup, 'Expiration')
            data['rep_id_no'] = self._extract_text_after_strong(soup, 'Rep')
            data['additional_information'] = self._extract_additional_info(soup)

            data['extraction_errors'] = self.extraction_errors.copy()

        except Exception as e:
            data['parsing_success'] = False
            data['extraction_errors'].append(f"Critical parsing error: {str(e)}")

        return data

    def _extract_company_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract company name, address, and contact information."""
        company_info = {}

        try:
            # Find the section with company information
            # Look for the pattern after "Plant Registry/Basic Permit/Brewers No"

            # Extract company lines - they appear as individual table rows
            company_lines = []

            # Look for table cells that contain company information
            cells = soup.find_all('td')
            in_company_section = False

            for cell in cells:
                cell_text = self._clean_text(cell.get_text())

                # Start capturing after we see the permit number pattern
                if re.match(r'^[A-Z]{2}-[A-Z]-\d+$', cell_text):  # e.g., "DC-I-418"
                    in_company_section = True
                    company_info['plant_registry_number'] = cell_text
                    continue

                # Capture company information lines
                if in_company_section and cell_text:
                    # Stop at contact information or other sections
                    if 'Contact Information:' in cell_text or 'Phone Number:' in cell_text:
                        break
                    company_lines.append(cell_text)

            # Parse company lines - map to expected applicant field names
            if company_lines:
                company_info['applicant_business_name'] = company_lines[0] if len(company_lines) > 0 else None
                company_info['address_line_1'] = company_lines[1] if len(company_lines) > 1 else None
                company_info['address_line_2'] = company_lines[2] if len(company_lines) > 2 else None

                # Combine address lines for full address (using expected schema field name)
                address_parts = [line for line in company_lines[1:] if line]
                company_info['applicant_mailing_address'] = ', '.join(address_parts) if address_parts else None

                # Also keep legacy names for backward compatibility
                company_info['company_name'] = company_info['applicant_business_name']
                company_info['full_address'] = company_info['applicant_mailing_address']

            # Extract contact information
            contact_info = self._extract_contact_info(soup)
            company_info.update(contact_info)

            # Extract fax number
            fax_number = self._extract_fax_number(soup)
            if fax_number:
                company_info['applicant_fax'] = fax_number

        except Exception as e:
            self.extraction_errors.append(f"Error extracting company info: {str(e)}")

        return company_info

    def _extract_contact_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract contact name and phone number."""
        contact_info = {}

        try:
            # Look for contact name pattern
            cells = soup.find_all('td')
            for i, cell in enumerate(cells):
                cell_text = self._clean_text(cell.get_text())

                # Look for name pattern (FIRST LAST)
                if re.match(r'^[A-Z]+\s+[A-Z]+$', cell_text) and len(cell_text.split()) == 2:
                    parts = cell_text.split()
                    contact_info['contact_first_name'] = parts[0]
                    contact_info['contact_last_name'] = parts[1]
                    contact_info['contact_full_name'] = cell_text

                # Look for phone number pattern
                phone_match = re.search(r'\((\d{3})\)\s*(\d{3})-(\d{4})', cell_text)
                if phone_match:
                    # Map to expected schema field names
                    contact_info['applicant_phone'] = phone_match.group(0)
                    contact_info['contact_phone_raw'] = phone_match.group(0)  # Keep legacy name
                    contact_info['contact_phone_area_code'] = phone_match.group(1)
                    contact_info['contact_phone_exchange'] = phone_match.group(2)
                    contact_info['contact_phone_number'] = phone_match.group(3)

        except Exception as e:
            self.extraction_errors.append(f"Error extracting contact info: {str(e)}")

        return contact_info

    def _extract_qualifications(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract the qualifications text block."""
        try:
            # Method 1: Find "TTB has not reviewed" text directly (most reliable)
            ttb_text = soup.find(string=re.compile('TTB has not reviewed', re.IGNORECASE))
            if ttb_text:
                return self._clean_text(ttb_text.strip())

            # Method 2: Find qualifications label and get text from next row
            qual_strong = soup.find('strong', string=re.compile('Qualifications:', re.IGNORECASE))
            if qual_strong:
                # Navigate up to the <tr> containing the label
                tr_parent = qual_strong.parent
                while tr_parent and tr_parent.name != 'tr':
                    tr_parent = tr_parent.parent

                if tr_parent:
                    # Get the next sibling row which contains the actual qualifications text
                    next_row = tr_parent.find_next_sibling('tr')
                    if next_row:
                        qual_text = self._clean_text(next_row.get_text())
                        if qual_text:
                            return qual_text

        except Exception as e:
            self.extraction_errors.append(f"Error extracting qualifications: {str(e)}")

        return None

    def _extract_additional_info(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract additional information/remarks from COLA detail page."""
        try:
            # Look for remarks, notes, additional info sections
            patterns = ['Additional Information', 'Remarks', 'Notes', 'Comments', 'Additional']
            for pattern in patterns:
                info = self._extract_text_after_strong(soup, pattern)
                if info:
                    return info
            return None
        except Exception as e:
            self.extraction_errors.append(f"Error extracting additional info: {str(e)}")
            return None


class CertificateParser(TTBDataExtractor):
    """Parser for TTB Certificate HTML pages."""

    def parse(self, html_content: str) -> Dict[str, Any]:
        """Parse certificate HTML and extract structured data."""
        soup = BeautifulSoup(html_content, 'html.parser')

        data = {
            'data_type': 'certificate',
            'extraction_errors': [],
            'parsing_success': True
        }

        try:
            # Basic identification
            data['ttb_id'] = self._extract_ttb_id(soup)
            data['serial_number'] = self._extract_field_by_number(soup, '4')  # Field 4: Serial Number

            # Product information
            data['brand_name'] = self._extract_field_by_number(soup, '6')  # Field 6: Brand Name
            data['fanciful_name'] = self._extract_field_by_number(soup, '7')  # Field 7: Fanciful Name
            data['grape_varietals'] = self._extract_field_by_number(soup, '10')  # Field 10: Grape Varietal(s)
            data['wine_appellation'] = self._extract_field_by_number(soup, '11')  # Field 11: Wine Appellation

            # Company information
            data['plant_registry_number'] = self._extract_field_by_number(soup, '2')  # Field 2

            # Contact information - map to expected schema field names
            contact_phone = self._extract_field_by_number(soup, '12')  # Field 12: Phone Number
            contact_email = self._extract_field_by_number(soup, '13')  # Field 13: Email
            contact_fax = self._extract_fax_number(soup)  # Try to extract fax from content

            data['applicant_phone'] = contact_phone
            data['applicant_email'] = contact_email
            data['applicant_fax'] = contact_fax
            # Keep legacy names for backward compatibility
            data['contact_phone'] = contact_phone
            data['contact_email'] = contact_email
            data['contact_fax'] = contact_fax

            # Dates
            data['application_date'] = self._extract_field_by_number(soup, '16')  # Field 16
            data['approval_date'] = self._extract_field_by_number(soup, '19')  # Field 19: Date Issued
            data['filing_date'] = self._extract_field_by_number(soup, '16')  # Same as application_date for certificates
            data['expiration_date'] = self._extract_field_by_number(soup, '20')  # Field 20 if exists

            # Extract checkbox information (nested for backward compatibility)
            checkbox_data = self._extract_checkboxes(soup)
            data.update(checkbox_data)

            # Also extract as consolidated string columns for fact_cola_applications
            source = checkbox_data.get('source_of_product', {})
            if source.get('domestic'):
                data['source_of_product'] = 'DOMESTIC'
            elif source.get('imported'):
                data['source_of_product'] = 'IMPORTED'
            else:
                data['source_of_product'] = None

            product = checkbox_data.get('product_type', {})
            if product.get('wine'):
                data['type_of_product'] = 'WINE'
            elif product.get('distilled_spirits'):
                data['type_of_product'] = 'DISTILLED SPIRITS'
            elif product.get('malt_beverage'):
                data['type_of_product'] = 'MALT BEVERAGE'
            else:
                data['type_of_product'] = None

            # Extract TTB codes as separate columns
            ttb_codes = self._extract_ttb_codes_flat(soup)
            data['ct_code'] = ttb_codes.get('ct_code')
            data['or_code'] = ttb_codes.get('or_code')
            # Keep nested structure for backward compatibility
            data['ttb_codes'] = ttb_codes

            # Extract image information
            data['label_images'] = self._extract_image_info(soup)

            # Extract company address
            company_info = self._extract_certificate_company_info(soup)
            data.update(company_info)

            # Extract applicant name from Field 18 (PRINT NAME OF APPLICANT OR AUTHORIZED AGENT)
            # or fall back to company info or Field 8
            data['applicant_name'] = (
                self._extract_field_by_number(soup, '18') or  # Field 18: Print name of applicant
                company_info.get('applicant_business_name') or
                self._extract_field_by_number(soup, '8')
            )

            # Extract signatures as separate columns
            signatures = self._extract_signatures_flat(soup)
            data['applicant_signature'] = signatures.get('applicant_signature')
            data['ttb_authorized_signature'] = signatures.get('authorized_signature_url')
            # Keep nested structure for backward compatibility
            data['signatures'] = signatures

            # Extract qualifications and status
            data.update(self._extract_certificate_status(soup))

            # Additional fields
            data['rep_id_no'] = self._extract_field_by_number(soup, '3') or self._extract_field_by_number(soup, '5')
            data['additional_information'] = self._extract_additional_info(soup)

            data['extraction_errors'] = self.extraction_errors.copy()

        except Exception as e:
            data['parsing_success'] = False
            data['extraction_errors'].append(f"Critical parsing error: {str(e)}")

        return data

    def _extract_ttb_id(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract TTB ID from certificate."""
        try:
            # Look for TTB ID in the header section
            ttb_div = soup.find('div', class_='data', string=re.compile(r'\d{14}'))
            if ttb_div:
                ttb_id_match = re.search(r'(\d{14})', ttb_div.get_text())
                if ttb_id_match:
                    return ttb_id_match.group(1)
        except Exception as e:
            self.extraction_errors.append(f"Error extracting TTB ID: {str(e)}")
        return None

    def _extract_field_by_number(self, soup: BeautifulSoup, field_num: str) -> Optional[str]:
        """Extract field value by field number (e.g., '2. PLANT REGISTRY...')."""
        try:
            # Look for field labels that start with the field number
            field_pattern = f"{field_num}\\."
            labels = soup.find_all(['div', 'td'], class_=re.compile('label|boldlabel'))

            for label in labels:
                label_text = label.get_text()
                if re.match(f"^{field_pattern}", label_text):
                    # Find the corresponding data div/cell
                    parent = label.parent
                    if parent:
                        # Look for data div in the same parent
                        data_div = parent.find('div', class_='data')
                        if data_div:
                            return self._clean_text(data_div.get_text())

        except Exception as e:
            self.extraction_errors.append(f"Error extracting field {field_num}: {str(e)}")
        return None

    def _extract_checkboxes(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract checkbox states from certificate."""
        checkbox_data = {}

        try:
            # Find all images that might be checkboxes
            checkbox_imgs = soup.find_all('img', alt=re.compile('checked|unchecked', re.IGNORECASE))

            # Group checkboxes by category
            source_of_product = {}
            product_type = {}
            application_type = {}

            for img in checkbox_imgs:
                alt_text = img.get('alt', '').lower()
                is_checked = 'checked' in alt_text

                # Try to find the associated label
                parent = img.parent
                while parent and not parent.get_text().strip():
                    parent = parent.parent

                if parent:
                    label_text = self._clean_text(parent.get_text()).lower()

                    # Categorize checkboxes
                    if 'domestic' in label_text:
                        source_of_product['domestic'] = is_checked
                    elif 'imported' in label_text:
                        source_of_product['imported'] = is_checked
                    elif 'wine' in label_text and 'distilled' not in label_text:
                        product_type['wine'] = is_checked
                    elif 'distilled spirits' in label_text:
                        product_type['distilled_spirits'] = is_checked
                    elif 'malt beverage' in label_text:
                        product_type['malt_beverage'] = is_checked
                    elif 'certificate of label approval' in label_text:
                        application_type['certificate_of_label_approval'] = is_checked
                    elif 'certificate of exemption' in label_text:
                        application_type['certificate_of_exemption'] = is_checked
                    elif 'distinctive liquor bottle' in label_text:
                        application_type['distinctive_liquor_bottle_approval'] = is_checked
                    elif 'resubmission' in label_text:
                        application_type['resubmission_after_rejection'] = is_checked

            if source_of_product:
                checkbox_data['source_of_product'] = source_of_product
            if product_type:
                checkbox_data['product_type'] = product_type
            if application_type:
                checkbox_data['application_type_checkboxes'] = application_type

        except Exception as e:
            self.extraction_errors.append(f"Error extracting checkboxes: {str(e)}")

        return checkbox_data

    def _extract_ttb_codes(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract CT and OR codes (legacy - returns nested dict)."""
        codes = self._extract_ttb_codes_flat(soup)
        return {'ttb_codes': codes} if codes else {}

    def _extract_ttb_codes_flat(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract CT and OR codes as flat dictionary."""
        codes = {}
        try:
            # Look for CT and OR codes in the header
            ct_label = soup.find('div', class_='label', string='CT')
            or_label = soup.find('div', class_='label', string='OR')

            if ct_label:
                ct_parent = ct_label.parent
                ct_data = ct_parent.find('div', class_='data')
                if ct_data:
                    codes['ct_code'] = self._clean_text(ct_data.get_text())

            if or_label:
                or_parent = or_label.parent
                or_data = or_parent.find('div', class_='data')
                if or_data:
                    codes['or_code'] = self._clean_text(or_data.get_text())

        except Exception as e:
            self.extraction_errors.append(f"Error extracting TTB codes: {str(e)}")

        return codes

    def _extract_image_info(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract label image information and URLs."""
        images = []

        try:
            # Look for images with publicViewAttachment URLs
            img_tags = soup.find_all('img', src=re.compile('publicViewAttachment'))

            for img in img_tags:
                image_info = {}

                # Extract URL
                image_info['original_url'] = img.get('src')

                # Extract alt text for image type
                image_info['alt_text'] = img.get('alt', '')
                image_info['type'] = self._clean_text(image_info['alt_text']).replace('Label Image: ', '')

                # Extract dimensions from img attributes
                width = img.get('width')
                height = img.get('height')
                if width and height:
                    image_info['display_dimensions'] = {
                        'width_pixels': int(width),
                        'height_pixels': int(height)
                    }

                # Try to extract physical dimensions from surrounding text
                physical_dims = self._extract_physical_dimensions(img)
                if physical_dims:
                    image_info['physical_dimensions'] = physical_dims

                # Extract filename from URL
                url = image_info['original_url']
                filename_match = re.search(r'filename=([^&]+)', url)
                if filename_match:
                    image_info['source_filename'] = filename_match.group(1)

                images.append(image_info)

        except Exception as e:
            self.extraction_errors.append(f"Error extracting image info: {str(e)}")

        return images

    def _extract_physical_dimensions(self, img_tag: Tag) -> Optional[Dict[str, float]]:
        """Extract physical dimensions from text near image."""
        try:
            # Look in parent elements for dimension text
            parent = img_tag.parent
            while parent:
                text = parent.get_text()
                # Look for pattern like "3.2 inches W X 3.7 inches H"
                dim_match = re.search(r'(\d+\.?\d*)\s*inches?\s*W\s*[Xx×]\s*(\d+\.?\d*)\s*inches?\s*H', text)
                if dim_match:
                    return {
                        'width_inches': float(dim_match.group(1)),
                        'height_inches': float(dim_match.group(2))
                    }
                parent = parent.parent

        except Exception as e:
            self.extraction_errors.append(f"Error extracting physical dimensions: {str(e)}")

        return None

    def _extract_certificate_company_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract company information from certificate."""
        company_info = {}

        try:
            # Field 8: Name and Address of Applicant
            # This is typically in a multi-line format
            field_8_label = soup.find(text=re.compile('8.*NAME AND ADDRESS OF APPLICANT'))

            if field_8_label:
                # Find the parent and look for data
                parent = field_8_label.parent
                while parent and parent.name != 'td':
                    parent = parent.parent

                if parent:
                    # Get all text from this cell, split by lines
                    text_lines = [self._clean_text(line) for line in parent.stripped_strings
                                  if not re.match(r'8\..*NAME AND ADDRESS', line)]

                    if text_lines:
                        # Map to expected applicant field names
                        company_info['applicant_business_name'] = text_lines[0]
                        if len(text_lines) > 1:
                            company_info['address_line_1'] = text_lines[1]
                        if len(text_lines) > 2:
                            company_info['address_line_2'] = text_lines[2]

                        # Combine address lines for mailing address field
                        address_parts = [line for line in text_lines[1:] if line]
                        company_info['applicant_mailing_address'] = ', '.join(address_parts) if address_parts else None

                        # Keep legacy names for backward compatibility
                        company_info['company_name'] = company_info['applicant_business_name']

        except Exception as e:
            self.extraction_errors.append(f"Error extracting company info: {str(e)}")

        return company_info

    def _extract_signatures(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract signature information (legacy - returns nested dict)."""
        signatures = self._extract_signatures_flat(soup)
        return {'signatures': signatures} if signatures else {}

    def _extract_signatures_flat(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract signature information as flat dictionary."""
        signatures = {}

        try:
            # Look for applicant signature (Field 17)
            applicant_sig = self._extract_field_by_number(soup, '17')
            if applicant_sig:
                signatures['applicant_signature'] = applicant_sig

            # Look for authorized signature image
            sig_img = soup.find('img', src=re.compile('publicViewSignature'))
            if sig_img:
                signatures['authorized_signature_url'] = sig_img.get('src')
                signatures['authorized_signature_present'] = True

                # Extract signature dimensions
                width = sig_img.get('width')
                height = sig_img.get('height')
                if width and height:
                    signatures['signature_dimensions'] = {
                        'width_pixels': int(width),
                        'height_pixels': int(height)
                    }
            else:
                signatures['authorized_signature_present'] = False

        except Exception as e:
            self.extraction_errors.append(f"Error extracting signatures: {str(e)}")

        return signatures

    def _extract_additional_info(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract additional information/remarks from certificate."""
        try:
            # Look for remarks, notes, additional info sections
            patterns = ['Additional Information', 'Remarks', 'Notes', 'Comments', 'Additional']
            for pattern in patterns:
                # Try using the base class method
                info = self._extract_text_after_strong(soup, pattern)
                if info:
                    return info

            # Also try field-based extraction
            for field_num in ['21', '22', '23']:  # Try various field numbers that might have additional info
                info = self._extract_field_by_number(soup, field_num)
                if info and len(info) > 10:  # Only if it has meaningful content
                    return info

            return None
        except Exception as e:
            self.extraction_errors.append(f"Error extracting additional info: {str(e)}")
            return None

    def _extract_certificate_status(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract status and qualifications from certificate."""
        status_info = {}

        try:
            # Look for status information
            status_text = soup.find(text=re.compile('THE STATUS IS'))
            if status_text:
                status_info['status'] = self._clean_text(status_text)

            # Look for class/type description
            class_type_text = soup.find(text=re.compile('TABLE.*WINE|DISTILLED SPIRITS|MALT BEVERAGE'))
            if class_type_text:
                status_info['class_type_code'] = self._clean_text(class_type_text)

            # Look for qualifications
            qual_text = soup.find(text=re.compile('TTB has not reviewed'))
            if qual_text:
                status_info['qualifications'] = self._clean_text(qual_text)

        except Exception as e:
            self.extraction_errors.append(f"Error extracting status: {str(e)}")

        return status_info


def create_parser(data_type: str) -> TTBDataExtractor:
    """Factory function to create appropriate parser based on data type."""
    if data_type == 'cola-detail':
        return COLADetailParser()
    elif data_type == 'certificate':
        return CertificateParser()
    else:
        raise ValueError(f"Unknown data type: {data_type}")


def parse_ttb_html(html_content: str, data_type: str) -> Dict[str, Any]:
    """
    Parse TTB HTML content and extract structured data.

    Args:
        html_content: Raw HTML content
        data_type: 'cola-detail' or 'certificate'

    Returns:
        Dictionary containing extracted data
    """
    parser = create_parser(data_type)
    return parser.parse(html_content)
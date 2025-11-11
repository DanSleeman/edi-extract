from edi import EdiReleaseDetails, FORECAST_CROSSREF, TIMING_CROSSREF, EdiBase, EdiDocument

# Date formats are inconsistent for each segment regardless of the format version
DATE_FORMATS = {
    '0020': '%y%m%d',
    '0030': '%y%m%d',
    '0040': '%Y%m%d',
    '0050': '%Y%m%d',
    '0060': '%Y%m%d',
}
START_SEGMENTS = {
                 # Record no # Doc Date
    '861':'BRA', # 1         #   2
    '810':'BIG', # 1         #   1
    '824':'BGN', # 2         #   3
    '997':'AK1', # 2         #   NA
    '856':'BSN', # 2         #   3
    '822':'BGN', # 2         #   3
    '862':'BSS', # 2         #   3
    '142':'BGN', # 2         #   3
    '853':'BGN', # 2         #   3
    '830':'BFR', # 3         #   Inconsistent. Many use 8. Tesla uses 8 as the "Horizon end date" and doesn't have a document creation date here.
    '850':'BEG', # 3         #   5
    '855':'BAK', # 3         #   4 or 9
    '860':'BCH', # 3         #   NA
    '864':'BMG', # ? There isn't really any element which matches a "record ID" value for these text message files
    '820':'TRN', # 2 NOT the 4th line in the message. Uses 5th line instead.
    '832':'BCT', # ? There isn't really any element which matches a "record ID" value for these price catalog files
}
class EdiX12(EdiBase):
    def __init__(self, element_separator: str=None, subelement_separator: str=None, segment_separator: str=None, **kwargs):
        super().__init__('X12', element_separator, subelement_separator, segment_separator, **kwargs)
    
    def handle_envelope(self, segment, state):
        # Should we use ISA or GS for the version check? GS seems to include a revision number i.e. 002001 or 002003 while ISA has just major version number i.e. 00200
        # Looking at some unique situations, Stellantis 822 documents have 00204 in the ISA segment, but 004040 in the GS segment.
        # They are using Ymd formatted dates inside the message.
        # 
        self.sender_id = self.universal_element_extract(segment, 6)
        self.sender_qualifier = self.universal_element_extract(segment, 5)
        self.receiver_id = self.universal_element_extract(segment, 8)
        self.receiver_qualifier = self.universal_element_extract(segment, 7)
        self.transaction_no = self.universal_element_extract(segment, 13)
        # self.edi_version = self.universal_element_extract(segment, 10) # ISA - 00401 / 00200
        # self.edi_version = self.universal_element_extract(segment, 8) # GS - 004010 / 002003
        # self.date_format = DATE_FORMATS.get(self.edi_version, '%Y%m%d')

    def handle_inner(self, segment, state):
        """
        Different formats of X12 standards have different date formats for date segments in the message.
            * 002004 is \\%y\\%m\\%d => 251031
            * 004010 is \\%Y\\%m\\%d => 20251031
        
        There is a different between ISA and GS versions. Some customers have an ISA version of 00200, but the GS version can be 006010.
        The GS segment defines the date format inside the message.
        """
        self.edi_version = self.universal_element_extract(segment, 8)[:4] # GS - 004010 / 002003 => 0040 / 0020
        self.date_format = DATE_FORMATS.get(self.edi_version, '%Y%m%d')
        state['document_issue_date'] = self.universal_element_extract(segment, 4, date=True, date_format_in=self.date_format)

    def handle_loop(self, segment, state):
        self.document_type = self.universal_element_extract(segment, 1)
        self.record_start_segment = START_SEGMENTS[self.document_type]
        
    def handle_start(self, segment, state):
        """
        Handle the start of a new EDI document (BFR segment).
        
        Assigns base attributes to an EdiDocument object assuming a standard format.

        * Record Number - Element 3
        * Horizon start date - Element 6
        * Horizon end date - Element 7
        * Document Issue Date - Element 8

        """
        GROUP_3 = ('BFR', 'BEG', 'BAK', 'BCH')
        GROUP_2 = ('BSN', 'BSS', 'BGN', 'AK1', 'TRN')
        GROUP_1 = ('BRA', 'BIG')
        if self.record_start_segment in GROUP_3:
            record_no_index = 3
        elif self.record_start_segment in GROUP_2:
            record_no_index = 2
        elif self.record_start_segment in GROUP_1:
            record_no_index = 1
        else:
            record_no_index = 0

        record_no = self.universal_element_extract(segment, record_no_index)

        # If an existing document is open, close it and store it
        if state["edi_class"]:
            state["edi_class_list"].append(state["edi_class"])
            state["edi_class"] = None

        # Start a new document
        edi_class = EdiDocument(record_no)
        edi_class.document_issue_date = state['document_issue_date']
        # These horizon dates may not exist.
        edi_class.horizon_start_date, edi_class.horizon_end_date = self.universal_element_extract(segment, [6,7], date=True, date_format_in=self.date_format)
        state["edi_class"] = edi_class

    def handle_address(self, segment, state):
        """
        Extract the address details. 

        Multiple address types exist for the N1 segment.

        * ST - Ship To Plant Code
        * SU - Supplier
        * SI - ???
        * SF - Ship From
        * MI - Material Issuer
        * MA - Material ???
        * MA - ???
        * BY - Buyer
        * VN - Vendor?
        * 16 - Ultimate Destination Code
        * II - Invoice Issuer (Assuming) - 810
        """
        address_type = self.universal_element_extract(segment,1)
        if address_type == 'ST':
            state["address"] = self.universal_element_extract(segment,4)
        if state["address"] and state["part_record"]:
            state["part_record"].plant = state["address"]

    def handle_accum(self, segment, state):
        accum_type = self.universal_element_extract(segment,1)
        if state["part_record"]:
            if accum_type == '01':
                q,d = self.universal_element_extract(segment,[2,4], date=True)
                state["part_record"].last_received_ship_quantity = q
                state["part_record"].last_received_ship_date = d
            elif accum_type == '02':
                a, s, e = self.universal_element_extract(segment, [2,4,6], date=True)
                state["part_record"].total_accum = a
                state["part_record"].total_accum_start_date = s
                state["part_record"].total_accum_end_date = e
            elif accum_type == 'PQ':
                a, s, e = self.universal_element_extract(segment, [3,5,2], date=True)
                state["part_record"].total_accum = a
                state["part_record"].total_accum_start_date = s
                state["part_record"].total_accum_end_date = e

class X12ReleaseDetails(EdiReleaseDetails):
    def __init__(self, date: str, quantity: str, rel_type: str, rel_timing: str, **kwargs):
        super().__init__('X12', date, quantity)

        self.rel_type = FORECAST_CROSSREF[self.__language__].get(rel_type, None)
        self.rel_timing = TIMING_CROSSREF[self.__language__].get(rel_timing, None)
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self):
        return f"X12ReleaseDetail(date={self.date}, quantity={self.quantity}, rel_type={self.rel_type}, rel_timing={self.rel_timing})"
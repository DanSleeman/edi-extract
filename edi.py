from datetime import datetime
from abc import ABC, abstractmethod
from warnings import warn

FORECAST_CROSSREF = {
    "X12":{
        "A":"Immediate",
        "B":"Pilot/Prevolume",
        "C":"Firm",
        "D":"Planning",
        "Z":"Mutually Defined"
    }
}
TIMING_CROSSREF = {
    "X12":{
        "D":"Discrete",
        "C":"Daily",
        "F":"Flexible Interval",
        "M":"Monthly Bucket",
        "W":"Weekly Bucket",
        "Z":"Mutually Defined"
    }
}


HEADER_DATES = {
    'X12':[]
    ,'EDIFACT':['137','158','159']
}


VALID_LANGUAGES = {'X12','EDIFACT'}
DEFAULT_SEPARATORS = {
    'X12' : {
        'ELEMENT':'*',
        'SEGMENT':'~',
        "SUBELEMENT":"<"
    },
    'EDIFACT' : {
        "ELEMENT":"+",
        "SEGMENT":"'",
        "SUBELEMENT":":"
    }
}

# These dictionaries are formatted using tuples since they work natively with str.startswith() methods and multiple options to match.
EDI_SEGMENTS = {
    "X12" : {
        "ENVELOPE":("ISA",),
        "INNER_MESSAGE":("GS",),
        "RECORD_START":("BFR",),
        "ADDRESS":("N1",),
        "LOOP":("ST",),
        "CLAIM_DETAILS":("N9",),
        "PART_DETAILS":("LIN",),
        "DATE_DETAILS":("DTM",),
        "SERVICE_LINE_DETAILS":("SSS",),
        "PROBLEM_RECORD_DETAILS":("PRR",),
        "MESSAGE":("MSG",),
        "TOTAL_CHARGE":("AMT",),
        "ACCUM":("SHP", "ATH"),
        "RELEASE":("FST",),
        "RELEASE_TYPE":("SDP",),
        "QUANTITY":("QTY",),
        "REFERENCE":("REF",),
        "FILE_END":("SE",)
    },
    "EDIFACT" : {
        "ENVELOPE":('UNB',),
        "INNER_MESSAGE":(),
        "RECORD_START":("UNH",),
        "ADDRESS":("NAD",),
        "LOOP":("BGM",),
        "CLAIM_DETAILS":(),
        "PART_DETAILS":("LIN",),
        "DATE_DETAILS":("DTM",),
        "SERVICE_LINE_DETAILS":(),
        "PROBLEM_RECORD_DETAILS":(),
        "MESSAGE":("MSG",),
        "TOTAL_CHARGE":(),
        "ACCUM":("",), # No specific accum segment. This is in a QTY segment with a specific qualifier.
        "RELEASE":("",), # No singular release segment. The release data is in DTM and QTY pairs making up date and quantity details.
        "RELEASE_TYPE":("SCC",),
        "QUANTITY":("QTY",),
        "REFERENCE":("REF",),
        "FILE_END":("UNT",)
    }
}


class ElementExtractionFailure(object):
    def __init__(self, failure_point: str, failure_reason: str, segment: str, position: int, element: str=None, subposition: int=None, date: bool=False, date_format_in: str='%Y%m%d', date_format_out: str='%m-%d-%Y'):
        self.failure_point = failure_point
        self.failure_reason = failure_reason
        self.segment = segment
        self.position = position
        self.element = element
        self.subposition = subposition
        self.date = date
        self.date_format_in = date_format_in
        self.date_format_out = date_format_out
    def __repr__(self):
        fields = [
            f"failure_point={self.failure_point!r}",
            f"failure_reason={self.failure_reason!r}",
            f"segment={self.segment!r}",
            f"position={self.position!r}",
        ]
        if self.element is not None:
            fields.append(f"element={self.element!r}")
        if self.subposition is not None:
            fields.append(f"subposition={self.subposition!r}")
        if self.date:
            fields.append(f"date_format_in={self.date_format_in!r}")
            fields.append(f"date_format_out={self.date_format_out!r}")
        return f"ElementExtractionFailure({', '.join(fields)})"
    def __str__(self):
        return self.segment
class EdiBase(ABC):
    def __init__(self, language: str, element_separator: str=None, subelement_separator: str=None, segment_separator: str=None, **kwargs):
        if language not in VALID_LANGUAGES:
            raise ValueError(f"{type(self).__name__} requires a language value of one of {VALID_LANGUAGES}. Received {language}.")
        self.language = language
        if not element_separator:
            self.element_separator = DEFAULT_SEPARATORS[self.language]["ELEMENT"]
        if not subelement_separator:
            self.subelement_separator = DEFAULT_SEPARATORS[self.language]["SUBELEMENT"]
        if not segment_separator:
            self.segment_separator = DEFAULT_SEPARATORS[self.language]["SEGMENT"]
        self.element_separator = self.element_separator.encode().decode('unicode_escape')
        self.subelement_separator = self.subelement_separator.encode().decode('unicode_escape')
        self.segment_separator = self.segment_separator.encode().decode('unicode_escape')
        self.default_segments()
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.DISPATCH_MAP = {
            self.envelope_segment: self.handle_envelope,
            self.inner_message_segment: self.handle_inner,
            self.record_start_segment: self.handle_start,
            self.address_segment: self.handle_address,
            self.part_details_segment: self.handle_part,
            self.release_segment: self.handle_release,
            self.accum_segment: self.handle_accum,
            self.file_end_segment: self.handle_end,
            self.loop_segment: self.handle_loop,
        }

    def handle_extraction_error(self, error):
        if not hasattr(self, 'extraction_errors'):
            self.extraction_errors = []
        self.extraction_errors.append(error)
        return error
    
    def default_segments(self):
        self.envelope_segment = EDI_SEGMENTS[self.language]["ENVELOPE"]
        self.inner_message_segment = EDI_SEGMENTS[self.language]["INNER_MESSAGE"]
        self.record_start_segment = EDI_SEGMENTS[self.language]["RECORD_START"]
        self.address_segment = EDI_SEGMENTS[self.language]["ADDRESS"]
        self.loop_segment = EDI_SEGMENTS[self.language]["LOOP"]
        self.claim_details_segment = EDI_SEGMENTS[self.language]["CLAIM_DETAILS"]
        self.part_details_segment = EDI_SEGMENTS[self.language]["PART_DETAILS"]
        self.date_details_segment = EDI_SEGMENTS[self.language]["DATE_DETAILS"]
        self.release_type_segment = EDI_SEGMENTS[self.language]["RELEASE_TYPE"]
        self.release_segment = EDI_SEGMENTS[self.language]["RELEASE"]
        self.qty_details_segment = EDI_SEGMENTS[self.language]["QUANTITY"]
        self.reference_segment = EDI_SEGMENTS[self.language]["REFERENCE"]
        self.accum_segment = EDI_SEGMENTS[self.language]["ACCUM"]
        self.service_line_details_segment = EDI_SEGMENTS[self.language]["SERVICE_LINE_DETAILS"]
        self.problem_record_details_segment = EDI_SEGMENTS[self.language]["PROBLEM_RECORD_DETAILS"]
        self.message_segment = EDI_SEGMENTS[self.language]["MESSAGE"]
        self.total_charge_segment = EDI_SEGMENTS[self.language]["TOTAL_CHARGE"]
        self.file_end_segment = EDI_SEGMENTS[self.language]["FILE_END"]



    def universal_element_extract(self, segment: str, position: int | tuple[int, int] | list | dict, *, date: bool=False, date_format_in: str='%Y%m%d', date_format_out: str='%m-%d-%Y'):
        """
        Extracts a single element from a given EDI segment based on the position/subposition index.
        
        Args:
            segment (str): Full EDI segment text (split by segment separator beforehand).
            position (int | tuple | list | dict):
                - int: element index
                - tuple: (element_index, subelement_index)
                - list: list of ints/tuples
                - dict: {label: int/tuple}
            date (bool, optional): If True, parse and reformat the value as a date.
            date_format_in (str, optional): Format for the input date string.
            date_format_out (str, optional): Format for the output date string.

        Returns:
            str | list[str] | dict[str, str]:
                Extracted value(s), optionally formatted as dates.
        """
        
        def extract_one(pos, *, force_date=None):
            if isinstance(pos, tuple):
                pos, subpos = pos
            else:
                subpos = None
            try:
                element = segment.split(self.element_separator)[pos]
            except IndexError:
                warn(f'Segment does not contain element at position {pos}: {segment}.')
                return self.handle_extraction_error(
                    ElementExtractionFailure(
                        failure_point='Element Index', 
                        failure_reason=f"Position {pos} out of range for {segment!r}", 
                        segment=segment, 
                        position=pos
                    )
                )
            if subpos != None:
                try:
                    element = element.split(self.subelement_separator)[subpos]
                except IndexError:
                    warn(f'Element does not contain subelement at position {subpos}: {element}')
                    return self.handle_extraction_error(
                        ElementExtractionFailure(
                            failure_point='Subelement Index', 
                            failure_reason=f"Subposition {subpos} out of range for {element!r}", 
                            segment=segment, 
                            position=position, 
                            element=element, 
                            subposition=subpos
                        )
                    )
            if date:
                try:
                    element = datetime.strptime(element, date_format_in).strftime(date_format_out)
                except Exception as e:
                    if single_date or force_date:
                        warn(f'Element does not match date format {element!r}: {date_format_in!r}.')
                        return self.handle_extraction_error(
                            ElementExtractionFailure(
                                failure_point='Date conversion', 
                                failure_reason=str(e), 
                                segment=segment, 
                                position=position, 
                                element=element, 
                                subposition=subpos, 
                                date_format_in=date_format_in, 
                                date_format_out=date_format_out
                            )
                        )
            return element
        
        single_date = True if isinstance(position, int) else False
        # --- Handle single ---
        if isinstance(position, (int, tuple)):
            return extract_one(position)

        # --- Handle list of positions ---
        elif isinstance(position, list):
            return [extract_one(p) for p in position]

        # --- Handle dict of named positions ---
        elif isinstance(position, dict):
            result = {}
            for key, spec in position.items():
                if isinstance(spec, dict):
                    # supports e.g. {"pos": 4, "date": True}
                    pos = spec.get("pos")
                    force_date = spec.get("date", None)
                else:
                    pos = spec
                    force_date = None
                result[key] = extract_one(pos, force_date=force_date)
            return result

        # --- Unsupported input ---
        else:
            raise TypeError(
                f"Unsupported type for position: {type(position).__name__}. "
                "Expected int, tuple, list, or dict."
            )
    @abstractmethod
    def handle_envelope(self, segment, state):...
    @abstractmethod
    def handle_inner(self, segment, state):...
    @abstractmethod
    def handle_loop(self, segment, state):...
    @abstractmethod
    def handle_start(self, segment, state):...
    @abstractmethod
    def handle_address(self, segment, state):...
    @abstractmethod
    def handle_part(self, segment, state):...
    @abstractmethod
    def handle_release(self, segment, state):...
    @abstractmethod
    def handle_accum(self, segment, state):...
    @abstractmethod
    def handle_end(self, segment, state):...

class EdiDocument(object):
    def __init__(self, ref_no: str, **kwargs):
        """
        Object representing an EDI document within an envelope.

        X12: ST to SE segments

        EDIFACT: UNH to UNT segments
        """
        self.ref_no = ref_no
        self.part_list = []
        for key, value in kwargs.items():
            setattr(self, key, value)
    def add_attr(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

class EdiPart(EdiDocument):
    def __init__(self, part_no: str, revision=None, **kwargs):
        self.part_no = part_no
        self.revision = revision
        self.part_rev = f"{self.part_no}-{self.revision}"
        self.release_list = []
        self.po = kwargs.get('po', None) or kwargs.get('customer_po', None)
        self.address = None
        self.total_accum = None
        self.total_accum_start_date = None
        self.total_accum_end_date = None
        for key, value in kwargs.items():
            setattr(self, key, value)
    def __repr__(self):
        return f"EdiPart(part_no={self.part_no}, revision={self.revision}, po={self.po})"
    def set_if_none(self, name: str, value) -> None:
        if getattr(self, name, None) is None:
            setattr(self, name, value)

class EdiReleaseDetails(object):
    def __init__(self, language: str, date: str, quantity: str, **kwargs):
        self.__language__ = language
        self.date = date
        self.quantity = quantity
        for key, value in kwargs.items():
            setattr(self, key, value)
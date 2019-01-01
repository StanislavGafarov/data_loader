import re

class NormalizePhoneNumbers(object):
    """
    Class which should merge mobile numbers into one token.
    """

    def __init__(self):
        boundary_start = '(?:[^\d\+]|^)'
        boundary_end = '(?:[^\d\-\:]|$)'
        phone_start = '(\+?[78]?'
        phone_delim = '[\s\-\(\)]?'
        phone_delim_with_dash = '[\) ][\- ]'

        t1 = phone_start + phone_delim + '\d{3}' + phone_delim + '\d{3}' + phone_delim + '\d{2}' + phone_delim + '\d{2})'
        t2 = phone_start + phone_delim + '\d{4}' + phone_delim + '\d{2}' + phone_delim + '\d{2}' + phone_delim + '\d{2})'
        t3 = phone_start + '\d{10}' + phone_delim + ')'
        t4 = phone_start + phone_delim + '\d{3}' + phone_delim_with_dash + '\d{3}' + phone_delim + '\d{2}' + phone_delim + '\d{2})'

        regex_phone_number_template = '|'.join([t1, t2, t3, t4])
        regex_phone_number_template = '{}(?:{}){}'.format(boundary_start, regex_phone_number_template, boundary_end)

        self.regex_phone_number = re.compile(regex_phone_number_template)
        self.plus_regex = re.compile(r"(\+ *)(\+7\d{10})")
        
    def flatten(self, lst: list) -> list:
        """
        Function merge list into list by decreasing hierarchy level.
        EXP: [ [1,2,3], [4,5,6], [[7,8],[9,10]] ] -> [1, 2, 3, 4, 5, 6, [7, 8], [9, 10]]
        """
        return [item for sublist in lst for item in sublist if item]
    
    def extract_only_numeric(self, text: str) -> str:
        return ''.join([letter for letter in text if letter in "0123456789"])

    def remove_additional_phone_pluses(self, text: str) -> str:
        final_text = re.sub(self.plus_regex, r'\2', text)
        return final_text

    def __call__(self, text: str) -> str:
        '''
        :param text (string)
        :return: text(string) (one token)
        '''
        already_done = []
        phone_numbers = self.flatten(self.regex_phone_number.findall(text))
        normalized_phone_numbers = []
        to_be_deleted = []
        for p in phone_numbers:
            digits = self.extract_only_numeric(p)[-10:]
            if digits[0] == "9":
                normalized_phone_numbers.append('+7' + digits)
            else:
                to_be_deleted.append(p)
        final_phone_numbers = [p for p in phone_numbers if p not in to_be_deleted]
        for normalized_pn, pn in zip(normalized_phone_numbers, final_phone_numbers):
            if (normalized_pn, pn.strip()) not in already_done:
                text = text.replace(pn.strip(), normalized_pn)
                already_done.append((normalized_pn, pn.strip()))
        text = self.remove_additional_phone_pluses(text)
        return text
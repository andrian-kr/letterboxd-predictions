import re

minutes_regex= r'(\d+)\xa0mins'
members_regex= r'(\d+)\xa0members'

def get_number_by_regex(text, search_regex):
    cleaned_text = re.sub(r'[\t\n,]', '', text)
    match = re.search(search_regex, cleaned_text)
    return int(match.group(1))

from requests_html import HTMLSession


# noinspection PyUnresolvedReferences
class Parser:
    def __init__(self, src_url):
        self.src_url = src_url
        self.session = HTMLSession()
        self.response = None

    def start_parsing(self, need_render=True):
        r = self.session.get(self.src_url)
        if need_render:
            r.html.render(timeout=60)
        self.response = r

    def get_xpath(self, xpath):
        result = self.response.html.xpath(xpath)
        return result

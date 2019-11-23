from selenium import webdriver
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
import urllib.request
import urllib.error as err
import requests
from termcolor import colored
import time
from bs4 import BeautifulSoup


class SeleniumParser:

    def __init__(self, target_url=None, start_parsing=True, no_css=False):
        self.target_url = target_url
        firefox_profile = FirefoxProfile()
        if no_css:
            firefox_profile.set_preference('permissions.default.stylesheet', 2)
        firefox_profile.set_preference('permissions.default.image', 2)
        firefox_profile.set_preference('javascript.enabled', False)
        firefox_profile.set_preference('dom.ipc.plugins.enabled.libflashplayer.so', 'false')
        firefox_profile.set_preference("http.response.timeout", 5)
        firefox_profile.set_preference("dom.max_script_run_time", 10)
        self.driver = webdriver.Firefox(firefox_profile)
        if start_parsing and target_url:
            self.__start_parsing()

    def __start_parsing(self):
        time.sleep(1)
        try:
            urllib.request.urlopen(self.target_url)
        except err.HTTPError as ex:
            print(colored(ex, 'yellow'))
            time.sleep(1)
            return False
        self.driver.get(self.target_url)
        return True

    def get_xpath(self, xpath):
        return self.driver.find_elements_by_xpath(xpath)

    @staticmethod
    def get_xpath_from(web_element, xpath, single=True):
        if single:
            return web_element.find_element_by_xpath(xpath)
        return web_element.find_elements_by_xpath(xpath)

    def click(self, xpath):
        self.driver.find_element_by_xpath(xpath).click()

    def get_url(self):
        return self.driver.current_url

    def reconnect(self, target_url, start_parsing=True):
        self.target_url = target_url
        if start_parsing:
            connected = self.__start_parsing()
            return connected

    def disconnect(self):
        self.driver.close()


class SoupParser:

    def __init__(self):
        self.__soup = None

    @property
    def soup(self) -> BeautifulSoup:
        return self.__soup

    @staticmethod
    def parse_str(input_str):
        return BeautifulSoup(input_str, "html.parser")

    def connect(self, url):
        page = requests.get(url)
        if page.status_code < 400:
            self.__soup = BeautifulSoup(page.text, "html.parser")
        else:
            raise ConnectionFailed("Failed to connect, info:\n"
                                   "url : {url}\n"
                                   "code : {code}".format(url=url,
                                                          code=page.status_code))


class ConnectionFailed(Exception):
    pass

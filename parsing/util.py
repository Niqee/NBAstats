from selenium import webdriver
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
import time


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
        self.driver.get(self.target_url)

    def get_xpath(self, xpath):
        return self.driver.find_elements_by_xpath(xpath)

    def click(self, xpath):
        self.driver.find_element_by_xpath(xpath).click()

    def get_url(self):
        return self.driver.current_url

    def reconnect(self, target_url, start_parsing=True):
        self.target_url = target_url
        if start_parsing:
            self.__start_parsing()

    def disconnect(self):
        self.driver.close()

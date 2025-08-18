from selenium.webdriver.common.by import By


class LocatorAvito:
    TITLES = (By.CSS_SELECTOR, "div[itemtype*='http://schema.org/Product']")
    NAME = (By.CSS_SELECTOR, "[itemprop='name']")
    DESCRIPTIONS = (By.CSS_SELECTOR, "p[style='--module-max-lines-size:4']")
    URL = (By.CSS_SELECTOR, "[itemprop='url']")
    PRICE = (By.CSS_SELECTOR, "[itemprop='price']")
    RGEO = (By.CSS_SELECTOR, ".xLPJ6")
    COMP = (By.CSS_SELECTOR, ".TTiHl")
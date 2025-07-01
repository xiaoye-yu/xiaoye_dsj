from DrissionPage import WebPage ,ChromiumOptions

from lxml import html
co = ChromiumOptions()
co.set_paths(r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe')

web_page = WebPage(chromium_options=co)
web_page.get("https://weather.cma.cn/web/weather/53868.html")
boost_root = html.fromstring(web_page.html)
elements = boost_root.xpath('/html/body')
if elements:
    target_div = elements[0]

    div_text = target_div.text_content().strip()

    print(div_text)
else:
    print("未匹配")
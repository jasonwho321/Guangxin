paths = {
    'Windows': {
        'Windows 10': {
            'webdriver_executable_path': r'C:\Users\Admin\Downloads\msedgedriver.exe',
            'binary_location': r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe',
            'SQLserver': r'D:\pythonproject\Guangxin\database\SQLserver.json',
            'airy_hib_account':r'D:\pythonproject\Guangxin\config_file\HIB_login_Airy.json',
            'password_file': r'C:\Users\Admin\Nutstore\1\我的坚果云\S数据分析\Wayfair账号密码.xlsx'
        },
        'Server 2019': {
            'webdriver_executable_path': r'C:\path\to\msedgedriver.exe',
            'binary_location': r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe',
            'SQLserver': r'D:\pythonproject\Guangxin\database\SQLserver.json',
            'password_file': r'C:\path\to\password.xlsx'
        }
    },
    'Linux': {
        'webdriver_executable_path': '/path/to/msedgedriver',
        'binary_location': '/path/to/msedge',
        'password_file': '/path/to/password.xlsx'
    },
    'Darwin': {  # MacOS
        'webdriver_executable_path': '/Users/huzhang/msedgedriver',
        'binary_location': '/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge',
        'SQLserver': '/Users/huzhang/PycharmProjects/Guangxin/database/SQLserver.json',
        'airy_hib_account':'/Users/huzhang/PycharmProjects/Guangxin/config_file/HIB_login_Airy.json',
        'password_file': '/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/我的坚果云/S数据分析/Wayfair账号密码.xlsx'
    }
}
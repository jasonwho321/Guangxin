import csv
import os
import pandas as pd
from base import convert_date
import requests
from upload_database import upload_to_sql_server
payments_header_ca = ['Wayfair Remittance #', 'Payment_Date', 'Invoice #', 'PO #', 'Invoice Date', 'Product Amount',
                      'Wayfair Allowance for Damages/ Defects (4%)', 'Wayfair Early Pay Discount (3%)', 'Shipping',
                      'Other', 'Tax/VAT', 'Payment Amount', 'Business', 'Order Type']
payments_header_us = ['Wayfair Remittance #', 'Payment_Date', 'Invoice #', 'PO #', 'Invoice Date', 'Product Amount',
                      'Wayfair Allowance for Damages/ Defects (4%)', 'Joss & Main Allowance for Damages/ Defects (4%)',
                      'Wayfair Early Pay Discount (3%)', 'Joss & Main Early Pay Discount (3%)', 'Shipping',
                      'Other', 'Tax/VAT', 'Payment Amount', 'Business', 'Order Type']
cookies_us = 'CSNUtId=23e17d3a-6358-954c-03bb-0714a782fb02; ExCSNUtId=23e17d3a-6358-954c-03bb-0714a782fb02; categoryId=45974; __ssid=47daaac1cfffd4224d3de54a75d33f8; cjConsent=MHxOfDB8Tnww; cjUser=c346a18e-a0cf-4903-ad0a-5e77133544a3; rskxRunCookie=0; rCookie=7485h7ezztm4u1o9ov3h7fl9ozs83q; CSNID=CC6A4CBD-5A09-4F60-AA35-8E2717132CB9; WFCS=CS9; _tt_enable_cookie=1; _ttp=0536d078-b85e-406c-97d0-2fbaecd08878; partner_home_language=eng_US; _px_f394gi7Fvmc43dfg_user_id=OWNmMzUyYTAtNTViZi0xMWVkLWI0ZDQtYzk2ZmJlZDA2MGUw; fs_cid=1.0; ibb=1; __pxvid=f2adad42-6b98-11ed-b4c3-0242ac120002; cjCountry=HK; _hjSessionUser_1670786=eyJpZCI6IjNhZWZhMjMwLTkyNzAtNWE5NS05M2VkLTJhOTliZTZkYzc0NCIsImNyZWF0ZWQiOjE2NzU4NTAyNjU2NTksImV4aXN0aW5nIjp0cnVlfQ==; sm_uuid=1679628501005; latestSearch=W004229762; _ga=GA1.3.1298624347.1666749777; CSN=g_countryCode%3DUS%26g_zip%3D12345; IR_PI=5e7fd087-ac7b-3e74-bc6d-2b7a16993791%7C1683793395489; lastRskxRun=1683706996259; _dpm_id.15e8=76abdef6-6ded-4b43-9750-2e718db06b57.1666749778.18.1683706998.1681442990.a6db7bac-5e78-45f6-9ac2-d07405655aee; _gcl_au=1.1.181972679.1683707008; otx=I+F9OmRbVEwWv09uygOBAg==; _ga_0GV7WXFNMT=GS1.1.1684461076.23.0.1684461076.60.0.0; CSN_CT=eyJhbGciOiJSUzI1NiIsImtpZCI6InBlRG5BMWVVVGRVQU00YVdjU3FnZm40ZEJhbFZCYnJ4R2ZEU0ZQYXVQbG8iLCJ0eXAiOiJKV1QiLCJ4NXQiOiIzQ2RlcE5rMGVZLUN2ai0zQ2JiT2F4U3ZfZEEiLCJqa3UiOiJodHRwczovL3Nzby5hdXRoLndheWZhaXIuY29tLy53ZWxsLWtub3duL29wZW5pZC1jb25maWd1cmF0aW9uL2p3a3MifQ.eyJuYmYiOjE2ODQ0Nzg2NjMsImV4cCI6MTY4NDQ4MDQ2NCwiaWF0IjoxNjg0NDc4NjY0LCJpc3MiOiJodHRwczovL3Nzby5hdXRoLndheWZhaXIuY29tLyIsImF1ZCI6ImF1dGhfaWRlbnRpdHkiLCJjbGllbnRfaWQiOiJ5NnZVV0dUd2dFVlA0VEFZbXZuTGxBIiwic3ViIjoiMTgyODIwNzQ3IiwicGx0Ijoid2ViIiwib3JpZ2luYWxfaWF0IjoiMTY2Njc0OTc4OSIsImN1aWQiOiI1NjY5Njk2NTk5Iiwicm9sZSI6InNmOmN1c3RvbWVyIiwidmVyc2lvbiI6IjIuMCIsImp0aSI6Inp0SVpQWVVzNUJPMmZ4UXRlTnhZbkEiLCJzY29wZSI6Im9wZW5pZCBvZmZsaW5lX2FjY2VzcyBzZjpyZWNvZ25pemVkIHNmOnRydXN0ZWQiLCJhbXIiOlsicGVyc2lzdGVuY2UiLCJnb29nbGVfb25ldGFwIiwicmVmcmVzaCJdfQ.ProwO5VIfbafYbZ9cag0eHKO1y-PMs7qga3VKIK917BN04xu4oVtLmCoa16uYPMRtcVHhwPptvoIU7prWuj8SFoYodsvbryNniGiqpnmyEZxNEnziW5603Xhd_6M_aq43NODDZpbt1wR6Q7N-mSvKTL2slflYIS5N9ZY77kzYCRB83V6RZDEe3d1DJxZbyMf_BufY6nuSgM5jhceRKwh6E8wcIGY-LNQNUjNgAQwvev6Vj94kRN5K2Bx5ytUm-Snrctic4EjVkP_D8htWpDoYe7IoTYlclokzpxUqXECPBiVpzk70S7T4swMM7i7BWeeqg5LD4o3NEx-bH7CGa_TsnoYFsD8CnkmQnscfNySnAuOP3MadHU_3Ip0UwmiKEauG7Lbzn3TMHzkE-ssi0s_kVq7bnwfwaCKjeolLcuxwYLHgDnP1adoTJJ2ZraxGBfunyI2dmDiLQjLiNvtVvz-PP7QbCsgfG-p3iKzzpToWiPWOlcKSdN56IaUEDwlTvc-rpeI_kskPjseAIWsHai3qZ_uAk68HaQq0CyVqVd8pjF2PdrIFMjlYDkSWhXXcOa_Yo_JhIB9qFbkKRvQ8LfRfJceasvFlCpDOo4V-H1eZrFr-ZImVQ3ngJ11MKdxSVdtT4DBiIjBI9jSEZ-vj77fmsdb3d2x81PSMrrdC_FOKCM; CSN_RT=pdn79DSr9Gfv2829GXNCJ2GvSmao5mSGZA_OPM9QmKc; CSNPersist=page_of_visit%3D437%26latestRefid%3DMKTEML_78661%26email_captured%3D1; extranet_WFSID=88532ceaa7be45121a217ccb543debed; CSNEXTUID=18029B28-25EF-44E3-A5E1-113A861AF9DC; MEDIAHUBPREVIEW=1; fs_uid=#WEEMY#5577655676555264:4728687131897856:::#71cce859#/1714116385; _gid=GA1.2.1321976945.1685331978; _gid=GA1.3.1321976945.1685331978; _gat_UA-2081664-5=1; supplierID=44345; _gat_UA-260345920-1=1; _ga=GA1.2.1298624347.1666749777; canary=0; _ga_W5CBQ28KZV=GS1.1.1685331978.16.1.1685335645.0.0.0; bearer_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc19hZG1pbiI6ZmFsc2UsImlzX2FkbWluX3ZpZXciOmZhbHNlLCJvcmdhbml6YXRpb25fYWNjb3VudF9uYW1lIjoiMzlmIEluYy4iLCJpc19zdXBwbGllcl9zZXR1cF9hY3RpdmUiOmZhbHNlLCJsYW5ndWFnZV9pZCI6ImVuZ19VUyIsImxpc3Rfc2VwYXJhdG9yIjoiLCIsImRhdGVfZm9ybWF0X2lkIjoyLCJudW1iZXJfZm9ybWF0X2lkIjoxLCJleHRyYW5ldF91c2VyX2lkIjoxNDQ1MzMsImV4dHJhbmV0X3VzZXJuYW1lIjoiRW1pbHkuTGluIiwiZXh0cmFuZXRfZmlyc3RfbmFtZSI6IkVtaWx5IiwiZXh0cmFuZXRfbGFzdF9uYW1lIjoiTGluIiwiZXh0cmFuZXRfZW1haWwiOiJlbWlseS5saW5AMzlmLm5ldCIsImV4dHJhbmV0X3VzZXJfdHlwZV9pZCI6MSwiZXh0cmFuZXRfdXNlcl9zdGF0dXMiOjEsInNjb3BlIjoie1wiYXBwbGljYXRpb25cIjpbXCJyZWFkOnNpdGVfcmVhZHlfYXNzZXRzXCIsXCJ3cml0ZTpzaXRlX3JlYWR5X2Fzc2V0c1wiLFwicmVhZDozZF9tb2RlbHNcIixcIndyaXRlOjNkX21vZGVsc1wiLFwicmVhZDpub25fc2l0ZV9yZWFkeV9hc3NldHNcIixcIndyaXRlOm5vbl9zaXRlX3JlYWR5X2Fzc2V0c1wiLFwicmVhZDpwZXJpZ29sZF9yZXN0cmljdGVkXCIsXCJ3cml0ZTpwZXJpZ29sZF9yZXN0cmljdGVkXCIsXCJyZWFkOmhndHZfcmVzdHJpY3RlZFwiLFwid3JpdGU6aGd0dl9yZXN0cmljdGVkXCIsXCJyZWFkOnZpc3VhbF9zZWFyY2hfcmVzdHJpY3RlZFwiLFwid3JpdGU6dmlzdWFsX3NlYXJjaF9yZXN0cmljdGVkXCIsXCJhZG1pbjpkYW1cIixcInJlYWQ6bWVyY2hfY29tcGxpYW5jZV9yZXN0cmljdGVkXCIsXCJ3cml0ZTptZXJjaF9jb21wbGlhbmNlX3Jlc3RyaWN0ZWRcIixcInJlYWQ6Y21lZGlhX2Vjb3N5c3RlbV90ZXN0aW5nXCJdLFwic3VwcGxpZXJzXCI6e1wiNDQzNDVcIjpbXCJyZWFkOmV4dHJhbmV0X3VzZXJcIixcInJlYWQ6aW52ZW50b3J5XCIsXCJ3cml0ZTppbnZlbnRvcnlcIixcInJlYWQ6Y2F0YWxvZ1wiLFwid3JpdGU6Y2F0YWxvZ1wiLFwicmVhZDpwdXJjaGFzZV9vcmRlclwiLFwid3JpdGU6cHVyY2hhc2Vfb3JkZXJcIixcInJlYWQ6bGFiZWxfZ2VuZXJhdGlvbl9ldmVudFwiLFwidmVyaWZpZWQ6c2hpcHBpbmdcIixcInZlcmlmaWVkOmFkdmFuY2Vfc2hpcF9ub3RpY2VcIixcInJlYWQ6cHJvZHVjdF9jbGFzc1wiLFwicmVhZDptYW51ZmFjdHVyZXJcIixcInJlYWQ6aW52b2ljZVwiLFwid3JpdGU6aW52b2ljZVwiLFwicmVhZDpzdXBwbGllcl9yZXZlbnVlXCIsXCJyZWFkOnNob3BfdGhlX2xvb2tcIixcIndyaXRlOmRyb3Bfc2hpcF9pbnZvaWNlXCIsXCJ2ZXJpZmllZDpkcm9wX3NoaXBfaW52b2ljZVwiLFwicmVhZDpjYXN0bGVnYXRlX3NjX29yZGVyc1wiLFwid3JpdGU6Y2FzdGxlZ2F0ZV9zY19vcmRlcnNcIixcInJlYWQ6c2VydmljZV9ib29raW5nXCIsXCJ3cml0ZTpzZXJ2aWNlX2Jvb2tpbmdcIixcInZlcmlmaWVkOnNlcnZpY2VfYm9va2luZ1wiLFwicmVhZDpmaW5hbmNlX3BhcnRuZXJfaG9tZVwiLFwid3JpdGU6ZmluYW5jZV9wYXJ0bmVyX2hvbWVcIl19fSIsImF1ZCI6Imh0dHBzOlwvXC9hcGkud2F5ZmFpci5jb21cLyIsInN1YiI6ImdwMDhLQzBqM00wQkhoRjdjRHhDbFhqTlNNTjl1a3NXQGNsaWVudHMiLCJpc3MiOiJodHRwczpcL1wvcGFydG5lcnMud2F5ZmFpci5jb21cLyIsImV4cCI6MTY4NTM0Mjg0NX0.aGw9wC-EvSrwl9x_6CqHSkl5929hWNEYdGk7TjwRDhY'
cookies_ca = 'CSNUtId=23e17d3a-6358-954c-03bb-0714a782fb02; ExCSNUtId=23e17d3a-6358-954c-03bb-0714a782fb02; categoryId=45974; __ssid=47daaac1cfffd4224d3de54a75d33f8; cjConsent=MHxOfDB8Tnww; cjUser=c346a18e-a0cf-4903-ad0a-5e77133544a3; rskxRunCookie=0; rCookie=7485h7ezztm4u1o9ov3h7fl9ozs83q; CSNID=CC6A4CBD-5A09-4F60-AA35-8E2717132CB9; WFCS=CS9; _tt_enable_cookie=1; _ttp=0536d078-b85e-406c-97d0-2fbaecd08878; partner_home_language=eng_US; _px_f394gi7Fvmc43dfg_user_id=OWNmMzUyYTAtNTViZi0xMWVkLWI0ZDQtYzk2ZmJlZDA2MGUw; fs_cid=1.0; ibb=1; __pxvid=f2adad42-6b98-11ed-b4c3-0242ac120002; cjCountry=HK; _hjSessionUser_1670786=eyJpZCI6IjNhZWZhMjMwLTkyNzAtNWE5NS05M2VkLTJhOTliZTZkYzc0NCIsImNyZWF0ZWQiOjE2NzU4NTAyNjU2NTksImV4aXN0aW5nIjp0cnVlfQ==; sm_uuid=1679628501005; latestSearch=W004229762; _ga=GA1.3.1298624347.1666749777; _pxvid=cb7b4741-e4cc-11ed-a1a5-546f61746261; CSN=g_countryCode%3DUS%26g_zip%3D12345; IR_PI=5e7fd087-ac7b-3e74-bc6d-2b7a16993791%7C1683793395489; lastRskxRun=1683706996259; _dpm_id.15e8=76abdef6-6ded-4b43-9750-2e718db06b57.1666749778.18.1683706998.1681442990.a6db7bac-5e78-45f6-9ac2-d07405655aee; _gcl_au=1.1.181972679.1683707008; otx=I+F9OmRbVEwWv09uygOBAg==; _ga_0GV7WXFNMT=GS1.1.1684461076.23.0.1684461076.60.0.0; CSN_CT=eyJhbGciOiJSUzI1NiIsImtpZCI6InBlRG5BMWVVVGRVQU00YVdjU3FnZm40ZEJhbFZCYnJ4R2ZEU0ZQYXVQbG8iLCJ0eXAiOiJKV1QiLCJ4NXQiOiIzQ2RlcE5rMGVZLUN2ai0zQ2JiT2F4U3ZfZEEiLCJqa3UiOiJodHRwczovL3Nzby5hdXRoLndheWZhaXIuY29tLy53ZWxsLWtub3duL29wZW5pZC1jb25maWd1cmF0aW9uL2p3a3MifQ.eyJuYmYiOjE2ODQ0Nzg2NjMsImV4cCI6MTY4NDQ4MDQ2NCwiaWF0IjoxNjg0NDc4NjY0LCJpc3MiOiJodHRwczovL3Nzby5hdXRoLndheWZhaXIuY29tLyIsImF1ZCI6ImF1dGhfaWRlbnRpdHkiLCJjbGllbnRfaWQiOiJ5NnZVV0dUd2dFVlA0VEFZbXZuTGxBIiwic3ViIjoiMTgyODIwNzQ3IiwicGx0Ijoid2ViIiwib3JpZ2luYWxfaWF0IjoiMTY2Njc0OTc4OSIsImN1aWQiOiI1NjY5Njk2NTk5Iiwicm9sZSI6InNmOmN1c3RvbWVyIiwidmVyc2lvbiI6IjIuMCIsImp0aSI6Inp0SVpQWVVzNUJPMmZ4UXRlTnhZbkEiLCJzY29wZSI6Im9wZW5pZCBvZmZsaW5lX2FjY2VzcyBzZjpyZWNvZ25pemVkIHNmOnRydXN0ZWQiLCJhbXIiOlsicGVyc2lzdGVuY2UiLCJnb29nbGVfb25ldGFwIiwicmVmcmVzaCJdfQ.ProwO5VIfbafYbZ9cag0eHKO1y-PMs7qga3VKIK917BN04xu4oVtLmCoa16uYPMRtcVHhwPptvoIU7prWuj8SFoYodsvbryNniGiqpnmyEZxNEnziW5603Xhd_6M_aq43NODDZpbt1wR6Q7N-mSvKTL2slflYIS5N9ZY77kzYCRB83V6RZDEe3d1DJxZbyMf_BufY6nuSgM5jhceRKwh6E8wcIGY-LNQNUjNgAQwvev6Vj94kRN5K2Bx5ytUm-Snrctic4EjVkP_D8htWpDoYe7IoTYlclokzpxUqXECPBiVpzk70S7T4swMM7i7BWeeqg5LD4o3NEx-bH7CGa_TsnoYFsD8CnkmQnscfNySnAuOP3MadHU_3Ip0UwmiKEauG7Lbzn3TMHzkE-ssi0s_kVq7bnwfwaCKjeolLcuxwYLHgDnP1adoTJJ2ZraxGBfunyI2dmDiLQjLiNvtVvz-PP7QbCsgfG-p3iKzzpToWiPWOlcKSdN56IaUEDwlTvc-rpeI_kskPjseAIWsHai3qZ_uAk68HaQq0CyVqVd8pjF2PdrIFMjlYDkSWhXXcOa_Yo_JhIB9qFbkKRvQ8LfRfJceasvFlCpDOo4V-H1eZrFr-ZImVQ3ngJ11MKdxSVdtT4DBiIjBI9jSEZ-vj77fmsdb3d2x81PSMrrdC_FOKCM; CSN_RT=pdn79DSr9Gfv2829GXNCJ2GvSmao5mSGZA_OPM9QmKc; CSNPersist=page_of_visit%3D437%26latestRefid%3DMKTEML_78661%26email_captured%3D1; extranet_WFSID=88532ceaa7be45121a217ccb543debed; CSNEXTUID=18029B28-25EF-44E3-A5E1-113A861AF9DC; MEDIAHUBPREVIEW=1; pxcts=ed2e41c3-fba8-11ed-9767-6958454c7859; fs_uid=#WEEMY#5577655676555264:4728687131897856:::#71cce859#/1714116385; _pxhd=AfLuv4BO76p-0gNqTCt3jsXRt9esr16qoq1HIgJwbxjU/Q816VAZkOKokIAWbs64mlyBjw1ViA7s7ELtLBfUtw==:eX-DViGPxpvov7h5ujAIoWF4/WYT/Hh/l71ms4elJwOLVhEpglFl5Ur4QgLzXx3Aq-xk8qt7CGE1KLixT7a8aaqqFDUg121ld8t0QktVz7M=; _gid=GA1.2.1321976945.1685331978; _gid=GA1.3.1321976945.1685331978; canary=0; supplierID=35722; _ga=GA1.2.1298624347.1666749777; session_timeout=1685342314; bearer_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc19hZG1pbiI6ZmFsc2UsImlzX2FkbWluX3ZpZXciOmZhbHNlLCJvcmdhbml6YXRpb25fYWNjb3VudF9uYW1lIjoiQ0FOXzM5RiBJbmMuIiwiaXNfc3VwcGxpZXJfc2V0dXBfYWN0aXZlIjpmYWxzZSwibGFuZ3VhZ2VfaWQiOiJlbmdfVVMiLCJsaXN0X3NlcGFyYXRvciI6IiwiLCJkYXRlX2Zvcm1hdF9pZCI6MiwibnVtYmVyX2Zvcm1hdF9pZCI6MSwiZXh0cmFuZXRfdXNlcl9pZCI6MTQ0NTMzLCJleHRyYW5ldF91c2VybmFtZSI6IkVtaWx5LkxpbiIsImV4dHJhbmV0X2ZpcnN0X25hbWUiOiJFbWlseSIsImV4dHJhbmV0X2xhc3RfbmFtZSI6IkxpbiIsImV4dHJhbmV0X2VtYWlsIjoiZW1pbHkubGluQDM5Zi5uZXQiLCJleHRyYW5ldF91c2VyX3R5cGVfaWQiOjEsImV4dHJhbmV0X3VzZXJfc3RhdHVzIjoxLCJzY29wZSI6IntcImFwcGxpY2F0aW9uXCI6W1wicmVhZDpzaXRlX3JlYWR5X2Fzc2V0c1wiLFwid3JpdGU6c2l0ZV9yZWFkeV9hc3NldHNcIixcInJlYWQ6M2RfbW9kZWxzXCIsXCJ3cml0ZTozZF9tb2RlbHNcIixcInJlYWQ6bm9uX3NpdGVfcmVhZHlfYXNzZXRzXCIsXCJ3cml0ZTpub25fc2l0ZV9yZWFkeV9hc3NldHNcIixcInJlYWQ6cGVyaWdvbGRfcmVzdHJpY3RlZFwiLFwid3JpdGU6cGVyaWdvbGRfcmVzdHJpY3RlZFwiLFwicmVhZDpoZ3R2X3Jlc3RyaWN0ZWRcIixcIndyaXRlOmhndHZfcmVzdHJpY3RlZFwiLFwicmVhZDp2aXN1YWxfc2VhcmNoX3Jlc3RyaWN0ZWRcIixcIndyaXRlOnZpc3VhbF9zZWFyY2hfcmVzdHJpY3RlZFwiLFwiYWRtaW46ZGFtXCIsXCJyZWFkOm1lcmNoX2NvbXBsaWFuY2VfcmVzdHJpY3RlZFwiLFwid3JpdGU6bWVyY2hfY29tcGxpYW5jZV9yZXN0cmljdGVkXCIsXCJyZWFkOmNtZWRpYV9lY29zeXN0ZW1fdGVzdGluZ1wiXSxcInN1cHBsaWVyc1wiOntcIjM1NzIyXCI6W1wicmVhZDpleHRyYW5ldF91c2VyXCIsXCJyZWFkOmludmVudG9yeVwiLFwid3JpdGU6aW52ZW50b3J5XCIsXCJyZWFkOmNhdGFsb2dcIixcIndyaXRlOmNhdGFsb2dcIixcInJlYWQ6cHVyY2hhc2Vfb3JkZXJcIixcIndyaXRlOnB1cmNoYXNlX29yZGVyXCIsXCJyZWFkOmxhYmVsX2dlbmVyYXRpb25fZXZlbnRcIixcInZlcmlmaWVkOnNoaXBwaW5nXCIsXCJ2ZXJpZmllZDphZHZhbmNlX3NoaXBfbm90aWNlXCIsXCJyZWFkOnByb2R1Y3RfY2xhc3NcIixcInJlYWQ6bWFudWZhY3R1cmVyXCIsXCJyZWFkOmludm9pY2VcIixcIndyaXRlOmludm9pY2VcIixcInJlYWQ6c3VwcGxpZXJfcmV2ZW51ZVwiLFwicmVhZDpzaG9wX3RoZV9sb29rXCIsXCJ3cml0ZTpkcm9wX3NoaXBfaW52b2ljZVwiLFwidmVyaWZpZWQ6ZHJvcF9zaGlwX2ludm9pY2VcIixcInJlYWQ6Y2FzdGxlZ2F0ZV9zY19vcmRlcnNcIixcIndyaXRlOmNhc3RsZWdhdGVfc2Nfb3JkZXJzXCIsXCJyZWFkOnNlcnZpY2VfYm9va2luZ1wiLFwid3JpdGU6c2VydmljZV9ib29raW5nXCIsXCJ2ZXJpZmllZDpzZXJ2aWNlX2Jvb2tpbmdcIixcInJlYWQ6ZmluYW5jZV9wYXJ0bmVyX2hvbWVcIixcIndyaXRlOmZpbmFuY2VfcGFydG5lcl9ob21lXCJdfX0iLCJhdWQiOiJodHRwczpcL1wvYXBpLndheWZhaXIuY29tXC8iLCJzdWIiOiJncDA4S0MwajNNMEJIaEY3Y0R4Q2xYak5TTU45dWtzV0BjbGllbnRzIiwiaXNzIjoiaHR0cHM6XC9cL3BhcnRuZXJzLndheWZhaXIuY29tXC8iLCJleHAiOjE2ODUzNDIzMTR9.YkrpBj9B1XgteF5yHmhLPzt3Xwd8a2tss04IuXg5Y74; _px3=fe77a4258f2c6f9c97f66d120ac661de205476a2eba353257fdecda9fb2674fb:ZkoM1MGCJo5oJi64YBKdADR/OW/KfUNs4rfzzg6g5TXbUaGVn2YJk5CY0JhRk+TZ/ZOSDWGOrTd03detIlANwQ==:1000:26ivSnXhXf+pZKq8TKrG81az0izju1fJln33dAJleFYWq92n+zfrTSSa9GZ+Q8X2CqnHILXlTnWW4BWC9NgG4zDmt3XSg42BdYdgohBA2Ro1dclGdrnikz/cSd+96bsbNJjFmoyEuaW40jh8eS4fjQsJIP+EcgG5MnPrH2C9lFagV7KnuHZBFdPsCZ/bXp78lfiV9TM7j+zmjaGoW3tsXg==; _dd_s=rum=1&id=21d130c1-c3d7-442f-8ab1-671cb67cc0f9&created=1685333074906&expire=1685336503495; _gat_UA-2081664-5=1; _ga_W5CBQ28KZV=GS1.1.1685331978.16.1.1685335603.0.0.0; dd_cookie_test_2c9dc03d-5fcd-4b31-a297-f4fa191c37d5=test'
csv_file_path_us = "/Users/huzhang/Downloads/Payments_Summary (1).csv"
csv_file_path_ca = "/Users/huzhang/Downloads/Payments_Summary.csv"


def download_wf_payment(country):
    # 创建一个文件夹来保存下载的文件
    # ！！！未完成国家区分，半手动，待更新
    if country == 'US':
        cookies = cookies_us
        csv_file_path = csv_file_path_us
        url_template = 'https://partners.wayfair.com/v/finance/payment/payments_summary/create_oms_csv?voucher_id={voucher_id}'
    else:
        cookies = cookies_ca
        csv_file_path = csv_file_path_ca
        url_template = "https://partners.wayfair.com/v/finance/payment/payments_summary/create_oms_csv?voucher_id={voucher_id}"
    output_folder = "downloaded_files_"+country
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # 读取CSV文件的第一列，获取voucher_ids
    voucher_ids = []
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,zh-TW;q=0.5',
        'cookie': cookies,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    with open(csv_file_path, "r") as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # 跳过标题行
        for row in reader:
            voucher_ids.append(int(row[0]))


    for voucher_id in voucher_ids:
        url = url_template.format(voucher_id=voucher_id)
        response = requests.get(url, headers=headers, stream=True)

        # 检查请求是否成功
        if response.status_code == 200:
            file_name = f"{output_folder}/voucher_{voucher_id}.csv"
            with open(file_name, "wb") as f:
                f.write(response.content)
            print(f"文件已下载：{file_name}")
        else:
            print(f"无法下载voucher_id为 {voucher_id} 的文件，错误代码：{response.status_code}")


def process_csv(input_file,country):
    # 读取CSV文件
    with open(input_file, mode='r') as csvfile:
        data = list(csv.reader(csvfile))

    # 初始化数据列表
    payments = []
    deductions = []
    print(input_file)
    # 查找关键信息
    remittance_number = data[0][0].split(': ')[-1].strip()

    if country == 'CA':
        if len(data[2]) > 1:
            payment_date = data[2][0].split(': ')[-1].strip() + " " + data[2][1].strip()
        else:
            print(
                f"The CSV file '{input_file}' does not have the expected format for the payment date. Skipping this file.")
            return [], []
    else:
        if len(data[2]) == 1:
            payment_date = data[2][0].split(': ')[-1].strip()
        else:
            print(
                f"The CSV file '{input_file}' does not have the expected format for the payment date. Skipping this file.")
            return [], []

    payments_start = None
    deductions_start = None
    for i, row in enumerate(data):
        if row and row[0] == 'Invoice #':
            payments_start = i
            break

    if payments_start is None:
        print(
            f"The CSV file '{input_file}' does not have the expected format for the payments start. Skipping this file.")
        return [], []

    for i, row in enumerate(data):
        if row and row[0] == 'Deduction':
            deductions_start = i
            break

    if deductions_start is None:
        print(
            f"The CSV file '{input_file}' does not have the expected format for the deductions start. Skipping this file.")
        return [], []

    if country == 'CA':
        payments_header = payments_header_ca
    else:
        payments_header = payments_header_us

    # 查找Payments表格的列名在表头中的索引
    col_name_to_idx = {col_name: idx for idx, col_name in enumerate(data[payments_start])}

    for row in data[payments_start + 1:deductions_start - 2]:
        if row:
            # 使用固定的表头初始化一个新的字典并将值设置为0
            payment_entry = {header: 0 for header in payments_header[2:]}

            # 遍历行中的值并将其插入到正确的字典项中
            for col_name, idx in col_name_to_idx.items():
                if col_name in payment_entry:
                    payment_entry[col_name] = row[idx]

            # 将字典转换为列表并将其添加到payments列表中
            payments.append([remittance_number, payment_date] + list(payment_entry.values()))

    # payments.append(['Wayfair Remittance #', 'Payment_Date'] + data[payments_start])
    # for row in data[payments_start+1:deductions_start-2]:
    #     if row:
    #         payments.append([remittance_number, payment_date] + row)

    deductions_header = ['Wayfair Remittance #', 'Payment_Date', 'Deduction ID', 'Deduction Date', 'Deduction Amount', 'Item', 'Item Qty', 'Customer', 'Reason', 'RA#', 'Description']
    deductions.append(deductions_header)

    deduction_entry = {header: '' for header in deductions_header[2:]}

    for row in data[deductions_start:]:
        if row:
            if row[0].startswith('Deduction'):
                if any(deduction_entry.values()):
                    deductions.append([remittance_number, payment_date] + list(deduction_entry.values()))
                    deduction_entry = {header: '' for header in deductions_header[2:]}
                if country == 'CA':
                    deduction_id, deduction_date, deduction_amount = row[1], row[2] + " " + row[3], row[-1] if len(row) > 4 else ''
                else:
                    deduction_id, deduction_date, deduction_amount = row[1], row[2], row[-1] if len(
                        row) > 4 else ''
                deduction_entry['Deduction ID'] = deduction_id
                deduction_entry['Deduction Date'] = deduction_date
                deduction_entry['Deduction Amount'] = deduction_amount
            else:
                for field in deductions_header[5:]:
                    for idx, value in enumerate(row):
                        if field == 'Item' and value.startswith(field):
                            value = row[idx].split(': ')[-1].strip()
                            deduction_entry[field] = value
                        elif field == 'Item Qty' and "Qty" in row[idx]:
                            value = row[idx].split(': ')[-1].strip()
                            deduction_entry[field] = value
                        elif field == 'Description' and row[idx].startswith("Desc"):
                            value = ' '.join(row[idx:]).split(': ')[-1].strip().replace('<br>', '')
                            deduction_entry[field] = value
                        elif value.startswith(field):
                            value = row[idx].split(': ')[-1].strip()
                            deduction_entry[field] = value

    if any(deduction_entry.values()):
        deductions.append([remittance_number, payment_date] + list(deduction_entry.values()))

    return payments, deductions


def read_and_merge_csv_files(file_folder):
    # 读取CSV文件
    deductions_ca = pd.read_csv(file_folder+'/deductions_CA.csv')
    deductions_us = pd.read_csv(file_folder+'/deductions_US.csv')
    payments_ca = pd.read_csv(file_folder+'/payments_CA.csv')
    payments_us = pd.read_csv(file_folder+'/payments_US.csv')
    # 添加国家列
    deductions_ca['Country'] = 'CA'
    deductions_us['Country'] = 'US'
    payments_ca['Country'] = 'CA'
    payments_us['Country'] = 'US'
    # 合并美国和加拿大的扣除数据
    deductions = pd.concat([deductions_ca, deductions_us], ignore_index=True)

    # 为加拿大数据添加两列并将值设置为0
    payments_ca['Joss & Main Allowance for Damages/ Defects (4%)'] = 0
    payments_ca['Joss & Main Early Pay Discount (3%)'] = 0

    # 按列名重新排序以匹配美国数据的列顺序
    payments_ca = payments_ca[payments_us.columns]

    # 合并美国和加拿大的支付数据
    payments = pd.concat([payments_ca, payments_us], ignore_index=True)

    # 清洗日期格式
    payments['Payment_Date'] = payments['Payment_Date'].apply(convert_date)
    payments['Invoice Date'] = payments['Invoice Date'].apply(convert_date)
    deductions['Payment_Date'] = deductions['Payment_Date'].apply(convert_date)
    deductions['Item Qty'] = deductions['Item Qty'].str.replace('"', '')  # 移除引号
    deductions['Item Qty'] = deductions['Item Qty'].astype(float)  # 转换为浮点数
    deductions['Deduction Amount'] = deductions['Deduction Amount'].astype(str).str.replace('"', '').astype(float)
    return payments, deductions


def main(folder_path,country):

    deductions_header = ['Wayfair Remittance #', 'Payment_Date', 'Deduction ID', 'Deduction Date', 'Deduction Amount', 'Item', 'Item Qty', 'Customer', 'Reason', 'RA#', 'Description']

    # 初始化总列表并添加表头
    if country == 'CA':
        all_payments = [payments_header_ca]
    else:
        all_payments = [payments_header_us]
    all_deductions = [deductions_header]

    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            input_file = os.path.join(folder_path, filename)
            payments, deductions = process_csv(input_file,country)

            # 将结果添加到总列表中
            all_payments.extend(payments[1:])  # 从第二行开始添加，跳过表头
            all_deductions.extend(deductions[1:])

    # 输出Payments和Deductions到CSV文件

    with open(f'/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/水单核对/payments_{country}.csv', mode='w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(all_payments)

    with open(f'/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/水单核对/deductions_{country}.csv', mode='w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(all_deductions)


if __name__ == '__main__':
    country_list = ['US','CA']
    for country in country_list:
        # download_wf_payment(country)
        folder_path = f"/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/水单核对/downloaded_files_{country}/"
        main(folder_path,country)

    payments, deductions = read_and_merge_csv_files(file_folder='/Users/huzhang/Library/CloudStorage/坚果云-john.hu@39f.net/「晓望集群」/S数据分析/水单核对')
    # payments.to_csv('payments.csv',index_label=False)
    # deductions.to_csv('deductions.csv',index_label=False)

    table_name_deductions = 'WF_Deductions'
    table_name_payments = 'WF_Payments'
    schema = "Finance"
    timestamp_column = 'Payment_Date'

    # upload_to_sql_server(payments,table_name=table_name_payments,schema=schema,timestamp_column=timestamp_column,if_exists='append_no_duplicates')
    upload_to_sql_server(deductions,table_name=table_name_deductions,schema=schema,timestamp_column=timestamp_column,if_exists='append_no_duplicates')
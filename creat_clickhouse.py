# --- START OF FILE creat_clickhouse.py ---

import requests
import random
import string
import time
import sys # <-- 1. إضافة استيراد للتعامل مع وسائط سطر الأوامر
from playwright.sync_api import sync_playwright, TimeoutError
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from clickhouse_driver import Client # <-- 2. إضافة استيراد للاتصال بـ ClickHouse
from clickhouse_driver.errors import ServerException

# =================================================================
# === الجزء الأول: دوال البريد المؤقت (لا تغيير هنا) ===
# =================================================================

def random_string(length=10):
    """توليد نص عشوائي."""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def create_temp_email_account():
    """
    إنشاء حساب بريد مؤقت، الحصول على التوكن، وإرجاع قاموس كامل.
    """
    print("\n--- بدء عملية إنشاء البريد الإلكتروني المؤقت ---")
    
    try:
        domains_resp = requests.get("https://api.mail.tm/domains")
        if domains_resp.status_code == 200:
            available_domains = [d['domain'] for d in domains_resp.json()['hydra:member']]
        else:
            available_domains = ["addy.biz", "mail.gw", "cold.fun"]
    except requests.exceptions.RequestException:
        print("⚠️ فشل الاتصال لجلب النطاقات، سيتم استخدام قائمة افتراضية.")
        available_domains = ["addy.biz", "mail.gw", "cold.fun"]

    while True:
        username = random_string()
        domain = random.choice(available_domains)
        email = f"{username}@{domain}"
        password = random_string(10) + "aA*1" 

        print(f"🔄 جاري محاولة إنشاء البريد: {email}")
        try:
            create_resp = requests.post("https://api.mail.tm/accounts", json={"address": email, "password": password})

            if create_resp.status_code == 201:
                print("✅ تم إنشاء الحساب بنجاح!")
                print("🔑 جاري الحصول على توكن المصادقة...")
                token_resp = requests.post("https://api.mail.tm/token", json={"address": email, "password": password})
                token = token_resp.json()["token"]
                headers = {"Authorization": f"Bearer {token}"}
                print("✅ تم الحصول على التوكن بنجاح.")
                return {"email": email, "password": password, "headers": headers}
            
            elif create_resp.status_code == 429:
                print("⚠️ طلبات كثيرة جدًا. سننتظر 30 ثانية...")
                time.sleep(30)
            else:
                print(f"⚠️ فشل إنشاء البريد (رمز الحالة: {create_resp.status_code}). نحاول مجددًا...")
                time.sleep(3)
        
        except requests.exceptions.RequestException as e:
            print(f"❌ حدث خطأ في الشبكة: {e}. ننتظر 10 ثواني...")
            time.sleep(10)

def wait_for_clickhouse_verification_link(headers, timeout=90):
    """
    ينتظر وصول رسالة من ClickHouse ويستخرج رابط التحقق منها.
    """
    print("\n--- ⏳ في انتظار وصول رسالة التحقق من ClickHouse... ---")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            messages_resp = requests.get("https://api.mail.tm/messages", headers=headers)
            messages = messages_resp.json()["hydra:member"]

            for msg in messages:
                if msg["from"]["address"] == "noreply@clickhouse.cloud":
                    print("📬 تم استلام رسالة من ClickHouse!")
                    msg_id = msg["id"]
                    msg_detail_resp = requests.get(f"https://api.mail.tm/messages/{msg_id}", headers=headers)
                    html_content = msg_detail_resp.json().get("html", [None])[0]

                    if html_content:
                        soup = BeautifulSoup(html_content, 'lxml')
                        verify_link_tag = soup.find('a', class_='action_button')
                        
                        if verify_link_tag and verify_link_tag.has_attr('href'):
                            verification_link = verify_link_tag['href']
                            print(f"🔗 تم العثور على رابط التحقق بنجاح!")
                            return verification_link
            time.sleep(5)
        except requests.exceptions.RequestException as e:
            print(f"حدث خطأ أثناء الاتصال بـ mail.tm: {e}")
            time.sleep(5)
    print("❌ لم تصل رسالة التحقق خلال المهلة المحددة (90 ثانية).")
    return None


# =================================================================
# === الجزء الثاني: دالة الأتمتة (معدلة لترجع البيانات) ===
# =================================================================

def run_signup_automation(account_details):
    """
    تنفذ عملية التسجيل الكاملة، وتُرجع بيانات الاتصال عند النجاح.
    """
    print("\n--- بدء الأتمتة باستخدام Playwright ---")
    
    email_address = account_details["email"]
    password_to_use = account_details["password"]
    headers = account_details["headers"]

    with sync_playwright() as p:
        # browser = p.chromium.launch(headless=True, slow_mo=50) # وضع التشغيل الصامت للسرعة
        browser = p.chromium.launch(headless=False, slow_mo=400) # وضع التشغيل المرئي للمراقبة
        page = browser.new_page()
        
        try:
            # ... (جميع مراحل الأتمتة من 1 إلى 6 تبقى كما هي) ...
            print("--- المرحلة 1: التسجيل الأولي ---")
            page.goto("https://auth.clickhouse.cloud/u/signup/", wait_until="domcontentloaded")
            page.locator(".c7c2d7b15").click()
            page.locator("#email").fill(email_address)
            page.locator("._button-signup-id").click()
            page.wait_for_selector("#password", timeout=15000)
            page.locator("#password").fill(password_to_use)
            page.locator("#terms-and-policies").check()
            page.locator(".cc757f1b2").click()
            
            print("\n--- المرحلة 2: التحقق من البريد ---")
            verification_link = wait_for_clickhouse_verification_link(headers)
            if not verification_link: raise Exception("لم يتم العثور على رابط التحقق.")
            page.goto(verification_link, wait_until="domcontentloaded")
            
            print("\n--- المرحلة 3: تسجيل الدخول ---")
            page.wait_for_selector("#username", timeout=20000)
            page.locator("#username").fill(email_address)
            page.locator("._button-login-id").click()
            page.locator("#password").fill(password_to_use)
            page.locator("._button-login-password").click()
            
            print("\n--- المرحلة 4: بدء النسخة التجريبية ---")
            page.wait_for_selector('[data-testid="start-trial-button-SCALE"]', state="visible", timeout=30000)
            page.locator('[data-testid="start-trial-button-SCALE"]').click()
            
            print("\n--- المرحلة 5: إعداد الخدمة ---")
            page.locator('[data-testid="select-trigger"]').first.click()
            page.locator('[data-testid="cloud-provider-option-gcp"]').click()
            page.locator('[data-testid="select-trigger"]').nth(1).click()
            page.get_by_text("Singapore (asia-southeast1)").click()
            
            print("\n--- المرحلة 6: إنشاء الخدمة ---")
            page.locator('[data-testid="create-service-button"]').click()
            
            print("\n--- المرحلة 7: استخراج بيانات الاتصال النهائية ---")

            print("- تخطي الاستبيان...")
            page.wait_for_selector('[data-testid="entry-questionnaire-skip-button"]', timeout=60000)
            page.locator('[data-testid="entry-questionnaire-skip-button"]').click()
            
            print("- الانتقال إلى الإعدادات...")
            page.get_by_text("Settings", exact=True).click()

            print("⌛️ في انتظار تفعيل زر إعادة تعيين كلمة المرور...")
            enabled_reset_button_selector = '[data-testid="reset-pwd-btn"]:not([disabled])'
            page.wait_for_selector(enabled_reset_button_selector, timeout=300000) 
            
            print("✅ تم تفعيل الزر. جاري الضغط عليه...")
            page.locator(enabled_reset_button_selector).click()

            print("- إظهار كلمة المرور الجديدة...")
            time.sleep(2)
            page.locator('[data-testid="password-display-eye-icon"]').click()
            password_element = page.locator('p[data-testid="container"].fs-exclude')
            new_ch_password = password_element.inner_text()
            
            print("- إغلاق النافذة المنبثقة...")
            page.locator('button:has(svg[aria-label="cross"])').click()
            time.sleep(2)  # الانتظار قليلاً للتأكد من إغلاق النافذة

            print("- الانتقال إلى لوحة التحكم المتقدمة...")
            page.locator('[data-testid="monitoringSidebarButton"]').click()
            page.locator('[data-testid="advancedDashboardSidebarButton"]').click()

            print("- استخراج رابط الهوست...")
            dashboard_link = page.get_by_role("link", name="native advanced dashboard.")
            href = dashboard_link.get_attribute("href")
            parsed_url = urlparse(href)
            hostname_with_port = parsed_url.netloc
            ch_host = hostname_with_port.split(':')[0]
            
            browser.close()
            # --- تم التعديل: إرجاع البيانات المستخرجة ---
            return ch_host, new_ch_password

        except Exception as e:
            print(f"\n❌ حدث خطأ غير متوقع أثناء الأتمتة: {e}")
            try:
                page.screenshot(path="error_screenshot.png")
                print("📸 تم حفظ لقطة شاشة باسم error_screenshot.png")
            except:
                pass
            browser.close()
            # --- تم التعديل: إرجاع None في حالة الفشل ---
            return None, None

# =================================================================
# === الجزء الثالث: دالة جديدة لتخزين البيانات في ClickHouse ===
# =================================================================

def store_credentials_in_clickhouse(main_db_host, main_db_user, main_db_password, data_to_store):
    """
    تتصل بقاعدة البيانات الرئيسية وتخزن بيانات الحساب الجديد.
    """
    print("\n--- 💾 بدء عملية تخزين البيانات في ClickHouse ---")
    
    # تفريغ البيانات المراد تخزينها في متغيرات واضحة
    temp_email = data_to_store["email"]
    temp_email_pass = data_to_store["email_pass"]
    new_host = data_to_store["host"]
    new_password = data_to_store["password"]

    client = None
    try:
        # الاتصال بقاعدة البيانات *الرئيسية* التي تحتوي على جدولنا
        client = Client(
            host=main_db_host,
            user=main_db_user,
            password=main_db_password,
            database='default',
            secure=True,
            port=9440
        )
        print("✅ تم الاتصال بقاعدة البيانات الرئيسية بنجاح.")

        table_name = 'CLICKHOUSE_TABLES'
        db_name = 'default'
        
        # إعداد البيانات للإدخال
        data_row = [{
            'CLICKHOUSE_MAIL': temp_email,
            'CLICKHOUSE_MAIL_PASS': temp_email_pass,
            'CLICKHOUSE_HOST': new_host,
            'CLICKHOUSE_PASSWORD': new_password
        }]

        # بناء جملة الإدخال
        insert_query = f"INSERT INTO {db_name}.{table_name} (CLICKHOUSE_MAIL, CLICKHOUSE_MAIL_PASS, CLICKHOUSE_HOST, CLICKHOUSE_PASSWORD) VALUES"

        # تنفيذ الإدخال
        client.execute(insert_query, data_row, types_check=True)
        
        print("\n" + "="*50)
        print("🎉🎉🎉 تم تخزين بيانات الحساب الجديد بنجاح في الجدول! 🎉🎉🎉")
        print("="*50)
        print(f"  📧 البريد المؤقت: {temp_email}")
        print(f"  🔑 كلمة سر البريد: {temp_email_pass}")
        print(f"  🌐 الهوست الجديد: {new_host}")
        print(f"  🔑 كلمة السر الجديدة: {new_password}")
        print("="*50)

    except ServerException as e:
        print(f"❌ خطأ من سيرفر ClickHouse أثناء التخزين: {e}")
    except Exception as e:
        print(f"❌ حدث خطأ غير متوقع أثناء التخزين في قاعدة البيانات: {e}")
    finally:
        if client:
            client.disconnect()
            print("🚪 تم إغلاق الاتصال بقاعدة البيانات الرئيسية.")

# =================================================================
# === الجزء الرابع: الدالة الرئيسية (معدلة لتنسيق العملية) ===
# =================================================================

if __name__ == "__main__":
    # 1. قراءة بيانات الاتصال بالقاعدة الرئيسية من وسائط سطر الأوامر
    if len(sys.argv) < 4:
        print("❌ خطأ: لم يتم تمرير بيانات الاتصال بالقاعدة الرئيسية.")
        print("يجب تشغيل هذا السكربت من خلال watcher.py")
        sys.exit(1) # الخروج من السكربت لأننا لا نستطيع المتابعة

    main_db_host = sys.argv[1]
    main_db_user = sys.argv[2]
    main_db_password = sys.argv[3]
    
    print("--- بدء سكربت إنشاء حساب ClickHouse ---")
    print(f"سيتم استخدام '{main_db_host}' لتخزين النتائج النهائية.")

    # 2. إنشاء حساب بريد مؤقت
    account_details = create_temp_email_account()
    
    if account_details:
        # 3. تشغيل الأتمتة واستقبال النتائج
        new_host, new_password = run_signup_automation(account_details)

        # 4. التحقق من نجاح الأتمتة وتخزين البيانات
        if new_host and new_password:
            # تجميع كل البيانات في قاموس واحد
            final_data = {
                "email": account_details["email"],
                "email_pass": account_details["password"],
                "host": new_host,
                "password": new_password
            }
            # استدعاء دالة التخزين
            store_credentials_in_clickhouse(main_db_host, main_db_user, main_db_password, final_data)
        else:
            print("\n❌ فشلت عملية الأتمتة، لن يتم تخزين أي بيانات.")
    else:
        print("\n❌ فشلت عملية إنشاء البريد، لا يمكن متابعة الأتمتة.")

    print("\n--- انتهى سكربت إنشاء الحساب ---")
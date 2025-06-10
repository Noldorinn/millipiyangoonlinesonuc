from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import logging
import time

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# WebDriver başlat
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

def wait_for_element(locator, timeout=10):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located(locator))

def get_latest_draw_info(df):
    if df.empty:
        return None
    try:
        latest_year = df["Yıl"].max()
        latest_year_draws = df[df["Yıl"] == latest_year]
        max_draw_number = int(latest_year_draws["Çekiliş No"].max())
        return max_draw_number
    except Exception as e:
        logger.warning(f"Veri analizinde hata: {e}")
        return None
    

def read_existing_data(file_path):
    try:
        xls = pd.ExcelFile(file_path)
        return {
            "SUPERLOTO": (pd.read_excel(xls, "Süper Loto"), None),
            "SAYISAL": (pd.read_excel(xls, "Sayısal Loto"), None),
            "ONNUMARA": (pd.read_excel(xls, "On Numara"), None),
            "SANSTOPU": (pd.read_excel(xls, "Şans Topu"), None),
        }
    except FileNotFoundError:
        logger.warning("Eski dosya bulunamadı, yeni dosya oluşturulacak.")
        return {
            "SUPERLOTO": (pd.DataFrame(), None),
            "SAYISAL": (pd.DataFrame(), None),
            "ONNUMARA": (pd.DataFrame(), None),
            "SANSTOPU": (pd.DataFrame(), None),
        }
    

def scrape_lottery_results(url, lottery_type, existing_df, last_draw_number):
    driver.get(url)
    logger.info(f"Veriler alınıyor: {url}")
    collected_data = []

    year_select = Select(wait_for_element((By.ID, "draw-year")))
    years = sorted([int(option.text) for option in year_select.options if not option.get_attribute("disabled")])
    
    for year in years:
        # Eğer Excel varsa (latest çekiliş varsa) ve bu yıl daha eskiyse → atla
        if last_draw_number and year < existing_df["Yıl"].max():
            continue

        Select(wait_for_element((By.ID, "draw-year"))).select_by_visible_text(str(year))
        time.sleep(1)

        Select(wait_for_element((By.ID, "draw-year"))).select_by_visible_text(str(year))
        time.sleep(1)
        month_select = Select(wait_for_element((By.ID, "draw-month")))
        months = [option.text for option in month_select.options if not option.get_attribute("disabled")]

        for month in months:
            Select(wait_for_element((By.ID, "draw-month"))).select_by_visible_text(month)
            time.sleep(1)

            try:
                wait_for_element((By.CLASS_NAME, "draws-submit")).click()
                time.sleep(3)
            except Exception as e:
                logger.error(f"Filtre hatası: {e}")
                continue

            try:
                results = driver.find_elements(By.CLASS_NAME, f"row.{lottery_type}")
            except Exception as e:
                logger.error(f"Sonuç bulma hatası: {e}")
                continue

            for result in results:
                try:
                    driver.execute_script("arguments[0].style.display = 'block';", result)
                    draw_number = int(result.find_element(By.CLASS_NAME, "draw_nr").text.strip())
                    if last_draw_number and draw_number <= last_draw_number:
                        continue

                    if lottery_type == "SUPERLOTO":
                        numbers = result.find_element(By.CLASS_NAME, "numbers-purple").find_elements(By.TAG_NAME, "div")
                        numbers_list = sorted([int(n.text.strip()) for n in numbers])
                        collected_data.append([draw_number] + numbers_list + [month, year])

                    elif lottery_type == "SAYISAL":
                        numbers = result.find_element(By.CLASS_NAME, "numbers").find_elements(By.TAG_NAME, "div")
                        main_numbers = sorted([int(n.text.strip()) for n in numbers[:-2]])
                        joker = numbers[-2].text.strip()
                        superstar = numbers[-1].text.strip()
                        collected_data.append([draw_number] + main_numbers + [joker, superstar, month, year])

                    elif lottery_type == "ONNUMARA":
                        numbers = result.find_element(By.CLASS_NAME, "numbers-onnumara").find_elements(By.CLASS_NAME, "number-onnumara")
                        numbers_list = sorted([int(n.text.strip()) for n in numbers])
                        collected_data.append([draw_number] + numbers_list + [month, year])

                    elif lottery_type == "SANSTOPU":
                        numbers = result.find_element(By.CLASS_NAME, "numbers-magenta").find_elements(By.TAG_NAME, "div")
                        main_numbers = sorted([int(n.text.strip()) for n in numbers[:-1]])
                        plus = numbers[-1].text.strip()
                        collected_data.append([draw_number] + main_numbers + [plus, month, year])

                    logger.info(f"Çekiliş No: {draw_number} | Sayılar: {numbers_list if 'numbers_list' in locals() else main_numbers}")

                except Exception as e:
                    logger.error(f"Çekiliş işleme hatası: {e}")
    return collected_data

def to_df_and_merge(new_data, old_df, columns):
    new_df = pd.DataFrame(new_data, columns=columns)
    combined_df = pd.concat([new_df, old_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=["Çekiliş No", "Yıl"], keep="first")
    combined_df = combined_df.sort_values(by=["Yıl", "Çekiliş No"], ascending=[False, False])
    return combined_df

def clean_numeric_columns(df, columns):
    for col in columns:
        df[col] = df[col].astype(str).str.replace("+", "", regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer')
    return df

if __name__ == "__main__":
    existing_data = read_existing_data("tum_loto_sonuclar.xlsx")
    
    # 🔎 En son çekiliş bilgilerini al ve anında terminale yazdır
    for key in existing_data:
        df = existing_data[key][0]
        latest = get_latest_draw_info(df)
        existing_data[key] = (df, latest)

        if key == "SUPERLOTO":
            logger.info(f"Süper Loto   ➤  En son çekiliş no: {latest}")
        elif key == "SAYISAL":
            logger.info(f"Sayısal Loto ➤  En son çekiliş no: {latest}")
        elif key == "ONNUMARA":
            logger.info(f"On Numara    ➤  En son çekiliş no: {latest}")
        elif key == "SANSTOPU":
            logger.info(f"Şans Topu    ➤  En son çekiliş no: {latest}")


    # En güncel çekiliş numaralarını belirle
    for key in existing_data:
        existing_data[key] = (existing_data[key][0], get_latest_draw_info(existing_data[key][0]))

    # Yeni verileri çek
    new_super = scrape_lottery_results("https://www.millipiyangoonline.com/cekilis-sonuclari/super-loto", "SUPERLOTO", *existing_data["SUPERLOTO"])
    new_sayisal = scrape_lottery_results("https://www.millipiyangoonline.com/cekilis-sonuclari/sayisal-loto", "SAYISAL", *existing_data["SAYISAL"])
    new_onnumara = scrape_lottery_results("https://www.millipiyangoonline.com/cekilis-sonuclari/on-numara", "ONNUMARA", *existing_data["ONNUMARA"])
    new_sanstopu = scrape_lottery_results("https://www.millipiyangoonline.com/cekilis-sonuclari/sans-topu", "SANSTOPU", *existing_data["SANSTOPU"])

    # Başlıklar
    super_loto_columns = ["Çekiliş No", "Sayı 1", "Sayı 2", "Sayı 3", "Sayı 4", "Sayı 5", "Sayı 6", "Ay", "Yıl"]
    sayisal_loto_columns = ["Çekiliş No", "Sayı 1", "Sayı 2", "Sayı 3", "Sayı 4", "Sayı 5", "Sayı 6", "Joker", "Süperstar", "Ay", "Yıl"]
    on_numara_columns = ["Çekiliş No"] + [f"Sayı {i}" for i in range(1, 23)] + ["Ay", "Yıl"]
    sans_topu_columns = ["Çekiliş No", "Sayı 1", "Sayı 2", "Sayı 3", "Sayı 4", "Sayı 5", "+ Sayı", "Ay", "Yıl"]

    # Verileri birleştir
    super_loto_df = to_df_and_merge(new_super, existing_data["SUPERLOTO"][0], super_loto_columns)
    sayisal_loto_df = to_df_and_merge(new_sayisal, existing_data["SAYISAL"][0], sayisal_loto_columns)
    on_numara_df = to_df_and_merge(new_onnumara, existing_data["ONNUMARA"][0], on_numara_columns)
    sans_topu_df = to_df_and_merge(new_sanstopu, existing_data["SANSTOPU"][0], sans_topu_columns)

    # Sayısal sütunlar
    super_loto_numeric_columns = ["Çekiliş No", "Sayı 1", "Sayı 2", "Sayı 3", "Sayı 4", "Sayı 5", "Sayı 6"]
    sayisal_loto_numeric_columns = ["Çekiliş No", "Sayı 1", "Sayı 2", "Sayı 3", "Sayı 4", "Sayı 5", "Sayı 6", "Joker", "Süperstar"]
    on_numara_numeric_columns = ["Çekiliş No"] + [f"Sayı {i}" for i in range(1, 23)]
    sans_topu_numeric_columns = ["Çekiliş No", "Sayı 1", "Sayı 2", "Sayı 3", "Sayı 4", "Sayı 5", "+ Sayı"]

    # Dönüştür
    super_loto_df = clean_numeric_columns(super_loto_df, super_loto_numeric_columns)
    sayisal_loto_df = clean_numeric_columns(sayisal_loto_df, sayisal_loto_numeric_columns)
    on_numara_df = clean_numeric_columns(on_numara_df, on_numara_numeric_columns)
    sans_topu_df = clean_numeric_columns(sans_topu_df, sans_topu_numeric_columns)

    # Excel'e yaz
    with pd.ExcelWriter("tum_loto_sonuclar.xlsx") as writer:
        super_loto_df.to_excel(writer, sheet_name="Süper Loto", index=False)
        sayisal_loto_df.to_excel(writer, sheet_name="Sayısal Loto", index=False)
        on_numara_df.to_excel(writer, sheet_name="On Numara", index=False)
        sans_topu_df.to_excel(writer, sheet_name="Şans Topu", index=False)

    logger.info("✅ Tüm çekiliş sonuçları başarıyla kaydedildi.")
    driver.quit()

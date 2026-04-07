from __future__ import annotations

import json
from copy import deepcopy
from typing import Any, Dict


DEFAULT_HOME_CONTENT: Dict[str, Any] = {
    "search": {
        "title": "Найти машину",
        "subtitle_suffix": "актуальных объявлений",
        "submit_label": "Найти",
        "submit_suffix": "объявлений",
        "reset_label": "Сбросить",
        "catalog_label": "Расширенный поиск",
    },
    "hero": {
        "stats_suffix": "авто в наличии",
        "title": "Импорт и подбор автомобилей из Европы и Азии",
        "subtitle": "Актуальные предложения от мировых площадок с ценой под ключ в одном каталоге",
        "note": "Возим проверенные авто из Европы, Азии и РФ. Минимальная предоплата — основная часть по факту поставки.",
        "benefits": [
            "От 10% предоплата",
            "Срок поставки от 10 дней",
            "Лизинг, Кредит, НДС, Безналичный расчет, Банковская ячейка",
        ],
        "actions": {
            "primary_label": "Привезённые авто",
            "secondary_label": "Заявка на расчёт",
        },
        "why_title": "Наши партнёры",
        "why_items": [
            "Сбер",
            "Альфа-Лизинг",
            "Европлан",
            "РЕСО-Лизинг",
            "ПСБ Лизинг",
            "Совкомбанк Лизинг",
        ],
    },
    "cases": {
        "title": "С 2019г. привезли более 1000 автомобилей под заказ",
        "subtitle": "Реальные кейсы клиентов",
    },
    "about": {
        "eyebrow": "О нас",
        "title": "Почему нужно обратиться именно в Level Avto?",
        "subtitle": "С 2019г. импортируем автомобили",
        "cards": [
            {
                "title": "Почему выбирают нас",
                "items": [
                    "Многолетний опыт в этом направлении и прозрачная работа на всех этапах",
                    "Вся работа доступна в социальных сетях и классифайдах",
                    "За более чем 7 лет работы — ни одного судебного иска",
                ],
            },
            {
                "title": "Ваши преимущества",
                "items": [
                    "Первый владелец в РФ и прозрачная история",
                    "Экономия 10–30% от цен в РФ",
                    "Большой выбор моделей, цветов и комплектаций",
                    "Дистанционное подписание и оплата",
                ],
            },
            {
                "title": "Услуги",
                "items": [
                    "Подбор и диагностика",
                    "Оплата через платежного агента",
                    "Логистика (все автомобили застрахованы)",
                    "Таможенная очистка (брокер)",
                    "ЭлПТС и постановка на учет",
                ],
            },
            {
                "title": "Сроки доставки",
                "items": [
                    "Европа: 10–45 дней (10 дней если объём двигателя до 1.9л)",
                    "Корея: 30–45 дней",
                    "Китай: 30–60 дней",
                    "ОАЭ: 30–45 дней",
                ],
                "paragraph": "Фиксируем стоимость и сроки в договоре без скрытых платежей.",
                "emphasis": "Локация и фото/видео отчет на всех этапах доставки.",
            },
        ],
        "leader": {
            "name": "Дмитрий Моторов",
            "role": "Руководитель компании Level Avto",
            "photo": "/static/img/leader-dmitry.png",
        },
    },
    "recommended": {
        "title": "Рекомендуем",
        "subtitle": "Отобранные предложения",
        "badge_label": "Рекомендуем",
        "empty_note": "Подборка появится автоматически при наличии подходящих авто.",
    },
    "vehicle_types": {
        "title": "Типы кузова",
        "subtitle": "Быстрый переход по кузовам",
        "empty_note": "Добавьте автомобили, чтобы показать статистику по кузовам.",
    },
    "advantages": {
        "title": "Преимущества Level Avto",
        "subtitle": "Сопровождаем подбор и поставку от поиска до выдачи",
        "cards": [
            {
                "title": "Честные источники",
                "text": "Собственный подбор, проверка, диагностика, логистика и выдача.",
            },
            {
                "title": "Финансовая гибкость",
                "text": "10% предоплата, помощь с лизингом, кредитом и НДС для бизнеса.",
            },
            {
                "title": "Под ключ",
                "text": "Проверка истории, диагностика, логистика, таможня и постановка на учёт.",
            },
        ],
    },
    "brands": {
        "title": "Популярные марки",
        "subtitle": "Показываем только марки, которые есть в каталоге",
        "empty_note": "Добавьте логотипы в static/img/brand-logos, чтобы они появились.",
    },
    "how_it_works": {
        "title": "Как мы работаем",
        "subtitle": "Прозрачно сопровождаем на каждом этапе",
        "steps": [
            {
                "title": "Бесплатный подбор",
                "text": "Уточняем бюджет и критерии, подбираем лучшие варианты под ваш запрос, проверяем по базам данных понравившийся вариант.",
            },
            {
                "title": "Проверка",
                "text": "Стоимость полного отчета одного автомобиля от 10000₽ до 40000₽ компанией автоподбора.",
            },
            {
                "title": "Договор поставки",
                "text": "Заключаем договор с фиксацией финальной стоимости и сроков под ключ в вашем городе, помогаем с выдачей кредита, лизинга или покупки на юридическое лицо с НДС.",
            },
            {
                "title": "Оплата",
                "text": "Полностью передаем НДС, возмещенный с покупки автомобиля за границей. Предоплата — от 10% от полной стоимости договора. Поэтапно: инвойс + комиссия за перевод, остаток при получении автомобиля на процедуре таможенной очистки.",
            },
            {
                "title": "Логистика",
                "text": "Отправляем автомобиль транспортной компанией-партнером с полной страховкой на всю стоимость. Вы получаете ссылку на геолокацию автомобиля 24/7 и фото/видео отчет на всех этапах.",
            },
            {
                "title": "Таможенная очистка",
                "text": "Таможенный брокер оформляет все документы для дальнейшего получения СБКТС и ЭлПТС.",
            },
            {
                "title": "Выдача",
                "text": "Передаем автомобиль, помогаем с регистрацией в ГИБДД, оформлением ОСАГО, КАСКО и дальнейшим гарантийным обслуживанием при наличии.",
            },
        ],
    },
    "seo": {
        "title": "Привозим автомобили из Европы и Азии под ваши задачи",
        "paragraphs": [
            "Level Avto — команда, которая специализируется на поставке автомобилей из Европы, Китая, Кореи, Японии и ОАЭ. Мы работаем с проверенными дилерами и профильными площадками, чтобы вы получали только честные предложения.",
            "Собираем подбор под бюджет и цели: семейные универсалы, первые автомобили, премиальные модели, электрокары. Помогаем с лизингом, кредитом и НДС.",
            "Мы берем на себя логистику, таможенное оформление, регистрацию и выдачу в вашем городе. Минимальная предоплата — основная часть оплачивается по факту поставки. Оставьте заявку, и мы предложим варианты в течение 1–2 дней.",
        ],
    },
    "lead": {
        "title": "Заявка на расчёт",
        "subtitle": "Оставьте контакты — подберём предложения и озвучим бюджет",
        "submit_label": "Отправить заявку",
        "note": "Заполните форму — свяжемся в течение рабочего дня",
        "privacy_prefix": "Я согласен с",
        "privacy_link_text": "политикой конфиденциальности",
    },
    "contacts": {
        "title": "Свяжитесь с нами",
        "subtitle_suffix": "Ежедневно 9:00–21:00",
        "callback_label": "Обратный звонок",
        "hours_note": "Каждый день 9:00–21:00",
        "call_button": "Позвонить",
        "email_button": "Написать на email",
    },
    "social": {
        "title": "Мы в социальных сетях и классифайдах",
        "items": [
            {"name": "Telegram", "url": "https://t.me/levelavto"},
            {"name": "Avito", "url": "https://www.avito.ru/brands/i113720154?src=sharing"},
            {"name": "Auto.ru", "url": "https://auto.ru/profile/28529976"},
            {"name": "VK", "url": "https://vk.ru/levelavto"},
        ],
    },
}


def _merge_dict(base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    for key, val in incoming.items():
        if isinstance(val, dict) and isinstance(base.get(key), dict):
            _merge_dict(base[key], val)
        else:
            base[key] = val
    return base


def default_home_content() -> Dict[str, Any]:
    return deepcopy(DEFAULT_HOME_CONTENT)


def build_home_content(content_map: Dict[str, str]) -> Dict[str, Any]:
    base = default_home_content()
    raw = content_map.get("home_content")
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                _merge_dict(base, data)
        except Exception:
            pass
    hero_title = content_map.get("hero_title")
    hero_subtitle = content_map.get("hero_subtitle")
    hero_note = content_map.get("hero_note")
    if hero_title:
        base["hero"]["title"] = hero_title
    if hero_subtitle:
        base["hero"]["subtitle"] = hero_subtitle
    if hero_note:
        base["hero"]["note"] = hero_note
    catalog_label = base.get("search", {}).get("catalog_label", "").strip()
    if not catalog_label or catalog_label.lower() == "к каталогу":
        base["search"]["catalog_label"] = DEFAULT_HOME_CONTENT["search"]["catalog_label"]
    cases_title = base.get("cases", {}).get("title", "").strip()
    if not cases_title or cases_title.lower() == "кейсы клиентов":
        base["cases"]["title"] = DEFAULT_HOME_CONTENT["cases"]["title"]
    cases_subtitle = base.get("cases", {}).get("subtitle", "").strip()
    if not cases_subtitle:
        base["cases"]["subtitle"] = DEFAULT_HOME_CONTENT["cases"]["subtitle"]
    hero_title_norm = (base.get("hero", {}).get("title") or "").strip().lower()
    if "европ" in hero_title_norm and "коре" in hero_title_norm:
        base["hero"]["title"] = DEFAULT_HOME_CONTENT["hero"]["title"]
    hero_sub_norm = (base.get("hero", {}).get("subtitle") or "").strip().lower()
    if "проверенных европейских" in hero_sub_norm:
        base["hero"]["subtitle"] = DEFAULT_HOME_CONTENT["hero"]["subtitle"]
    elif "в одном каталог" in hero_sub_norm:
        base["hero"]["subtitle"] = (
            str(base["hero"]["subtitle"])
            .replace("в одном каталог.", "в одном каталоге")
            .replace("в одном каталог", "в одном каталоге")
            .rstrip(".")
        )
    cases_title_norm = (base.get("cases", {}).get("title") or "").strip().lower()
    if cases_title_norm.startswith("за 8 лет привезли"):
        base["cases"]["title"] = DEFAULT_HOME_CONTENT["cases"]["title"]
    about_subtitle_norm = (base.get("about", {}).get("subtitle") or "").strip().lower()
    if about_subtitle_norm == "с 2019г. занимаюсь импортом автомобилей по всему миру.":
        base["about"]["subtitle"] = DEFAULT_HOME_CONTENT["about"]["subtitle"]
    return base


def serialize_home_content(home_content: Dict[str, Any]) -> str:
    return json.dumps(home_content, ensure_ascii=False, indent=2)

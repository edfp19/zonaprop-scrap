"use strict";

const BUENOS_AIRES = [-34.6037, -58.3816];
const CARD_OFFSET = [0, -12];
const HIDE_DELAY_MS = 350; // grace period so the pointer can travel from the pin onto the card

const $ = (id) => document.getElementById(id);
const number = (value, digits = 0) =>
  value.toLocaleString("es-AR", { minimumFractionDigits: digits, maximumFractionDigits: digits });

/* ---------- data helpers ---------- */

function relation(row) {
  const d = row.discount_percent;
  if (typeof d !== "number" || !Number.isFinite(d) || d === 0) return "neutral";
  return d > 0 ? "below" : "above";
}

function hasLocation(row) {
  const { latitude: lat, longitude: lng } = row;
  return (
    typeof lat === "number" && typeof lng === "number" &&
    Math.abs(lat) <= 90 && Math.abs(lng) <= 180
  );
}

function safeUrl(value) {
  if (typeof value !== "string") return null;
  try {
    const url = new URL(value);
    return url.protocol === "https:" || url.protocol === "http:" ? url.href : null;
  } catch {
    return null;
  }
}

function usd(value, row) {
  if (typeof value !== "number") return "—";
  return `US$ ${number(value)}${row.operation === "ALQUILER" ? "/mes" : ""}`;
}

function discountText(row) {
  const rel = relation(row);
  if (typeof row.discount_percent !== "number") return "Sin referencia";
  if (rel === "neutral") return "En el promedio";
  const magnitude = Math.abs(row.discount_percent);
  const pct = magnitude < 0.1 ? "<0,1" : number(magnitude, 1);
  return rel === "below" ? `${pct}% bajo el promedio` : `${pct}% sobre el promedio`;
}

/* ---------- DOM helpers (textContent only: source strings are never parsed as HTML) ---------- */

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function buildCard(row) {
  const rel = relation(row);
  const link = safeUrl(row.url);
  const photo = safeUrl(row.photo_url);
  const card = el("article", "card");

  const frame = el(link ? "a" : "div", "card-photo");
  if (link) {
    frame.href = link;
    frame.target = "_blank";
    frame.rel = "noopener noreferrer";
    frame.tabIndex = -1; // the call-to-action below is the keyboard stop
    frame.setAttribute("aria-hidden", "true");
  }
  const fallback = () => frame.replaceChildren(el("div", "no-photo", "Sin foto disponible"));
  if (photo) {
    const img = document.createElement("img");
    img.alt = "";
    img.loading = "lazy";
    img.referrerPolicy = "no-referrer";
    img.addEventListener("error", fallback, { once: true });
    img.src = photo;
    frame.append(img);
  } else {
    fallback();
  }
  card.append(frame);

  const body = el("div", "card-body");
  body.append(el("h3", "", row.title || "Propiedad sin título"));
  const where = [row.area_name, row.parent_area_name].filter(Boolean).join(", ");
  const facts = [
    row.property_type,
    typeof row.rooms === "number" ? `${row.rooms} amb.` : null,
    typeof row.covered_area_m2 === "number" ? `${number(row.covered_area_m2)} m² cub.` : null,
  ].filter(Boolean);
  body.append(el("p", "where", [where, facts.join(" · ")].filter(Boolean).join(" — ")));

  body.append(el("span", `badge ${rel}`, discountText(row)));
  if (row.is_opportunity) body.append(el("span", "badge ok", "Candidata (muestra suficiente)"));
  else if (!row.has_sufficient_data) body.append(el("span", "badge warn", "Muestra pequeña"));
  const visibility = (row.geolocation_visibility || "").toUpperCase();
  if (!visibility) body.append(el("span", "badge warn", "Precisión de ubicación desconocida"));
  else if (visibility !== "EXACT") body.append(el("span", "badge warn", "Ubicación aproximada"));

  const prices = el("dl", "prices");
  const addPrice = (label, value) => prices.append(el("dt", "", label), el("dd", "", value));
  addPrice("Precio pedido", usd(row.price_usd, row));
  addPrice("Promedio comparables", usd(row.comparable_average_usd, row));
  if (typeof row.price_per_m2_usd === "number") addPrice("USD por m²", `US$ ${number(row.price_per_m2_usd)}`);
  body.append(prices);

  const n = row.comparable_count;
  let note;
  if (!n) note = "No hay comparables para dar una referencia.";
  else if (row.has_sufficient_data) note = `Promedio de ${n} propiedades comparables.`;
  else note = `Promedio de solo ${n} comparables: referencia orientativa.`;
  body.append(el("p", "note", note));

  if (link) {
    const cta = el("a", "cta", "Ver en Zonaprop ↗");
    cta.href = link;
    cta.target = "_blank";
    cta.rel = "noopener noreferrer";
    body.append(cta);
  } else {
    body.append(el("p", "note", "El enlace al aviso no está disponible."));
  }
  card.append(body);
  return card;
}

function buildPin(row) {
  const rel = relation(row);
  const glyph = { below: "▼", above: "▲", neutral: "●" }[rel];
  return el("span", `pin ${rel}`, glyph);
}

/* ---------- map ---------- */

const map = L.map("map", { zoomControl: false }).setView(BUENOS_AIRES, 12);
// CARTO's free raster tiles now demand an API key, so use OSM's standard layer (light use only).
L.control.zoom({ position: "bottomright" }).addTo(map);
L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 19,
  attribution:
    '&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener noreferrer">OpenStreetMap</a> contributors',
}).addTo(map);

const cluster = L.markerClusterGroup({
  maxClusterRadius: 40,
  spiderfyOnMaxZoom: true, // pins with the same coordinates fan out at the deepest zoom
  showCoverageOnHover: false,
  iconCreateFunction: (group) => {
    const children = group.getAllChildMarkers();
    const below = children.filter((marker) => relation(marker.row) === "below").length;
    const above = children.filter((marker) => relation(marker.row) === "above").length;
    const greenEnd = below / children.length * 100;
    const redEnd = (below + above) / children.length * 100;
    const badge = el("span", "", String(children.length));
    badge.style.background = `conic-gradient(var(--below) 0% ${greenEnd}%, var(--above) ${greenEnd}% ${redEnd}%, var(--neutral) ${redEnd}% 100%)`;
    badge.title = `${below} por debajo · ${above} por encima · ${children.length - below - above} sin diferencia o referencia`;
    return L.divIcon({
      html: badge,
      className: "cluster-icon",
      iconSize: [38, 38],
    });
  },
}).addTo(map);

/* Hover card: one shared popup. It stays open while the pointer is on the pin OR the card. */
const popup = L.popup({
  className: "listing-popup",
  closeButton: true,
  autoPan: false, // hovering must never move the map from under the pointer
  autoPanPaddingTopLeft: [20, 90], // clear the floating filter bar
  autoPanPaddingBottomRight: [20, 60], // clear the legend
  offset: CARD_OFFSET,
  minWidth: 280,
  maxWidth: 280,
});
let hideTimer = 0;
let pinned = false;
let activeMarker = null;

const cancelHide = () => clearTimeout(hideTimer);
const scheduleHide = () => {
  cancelHide();
  if (!pinned) hideTimer = setTimeout(() => map.closePopup(popup), HIDE_DELAY_MS);
};

/* Cards open above the pin; when that would be clipped (or hidden under the floating filter
   bar), flip below it, choosing whichever side has more room. */
function placeCard(marker) {
  const wrapper = popup.getElement()?.querySelector(".leaflet-popup-content-wrapper");
  if (!wrapper) return;
  let height = wrapper.offsetHeight;
  const point = map.latLngToContainerPoint(marker.getLatLng());
  const bar = $("filters").getBoundingClientRect();
  const mapBox = $("map").getBoundingClientRect();
  const overlapsBar = point.x + 140 + mapBox.left > bar.left && point.x - 140 + mapBox.left < bar.right;
  const top = overlapsBar ? bar.bottom - mapBox.top + 8 : 8;
  const available = Math.max(point.y - 13 - top, map.getSize().y - point.y - 21);
  if (height > available) {
    wrapper.style.maxHeight = `${Math.max(100, available)}px`;
    wrapper.style.overflowY = "auto";
    height = wrapper.offsetHeight;
  }
  const roomAbove = point.y - 13 - height - top;
  const roomBelow = map.getSize().y - 8 - (point.y + 13 + height);
  if (roomAbove >= 0 || roomBelow <= roomAbove) return;
  popup.getElement().classList.add("below-pin");
  popup.options.offset = [0, height + 34];
  popup.update();
}

function showCard(marker, { pin }) {
  cancelHide();
  if (activeMarker !== marker) {
    activeMarker = marker;
    pinned = false;
    popup.options.autoPan = false;
    popup.options.offset = CARD_OFFSET;
    popup.setLatLng(marker.getLatLng()).setContent(buildCard(marker.row)).openOn(map);
    placeCard(marker);
  }
  if (pin) {
    pinned = true;
    popup.options.autoPan = true; // touch/click: bring the whole card into view
    popup.update();
    popup.options.autoPan = false;
  }
}

map.on("popupopen", (event) => {
  const node = event.popup.getElement();
  node.addEventListener("mouseenter", cancelHide);
  node.addEventListener("mouseleave", scheduleHide);
});
map.on("popupclose", () => {
  cancelHide();
  pinned = false;
  const marker = activeMarker;
  activeMarker = null;
  if (marker && popup.getElement()?.contains(document.activeElement)) marker.getElement()?.focus();
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") map.closePopup(popup);
});

function buildMarker(row) {
  const marker = L.marker([row.latitude, row.longitude], {
    icon: L.divIcon({ html: buildPin(row), className: "pin-wrap", iconSize: [26, 26] }),
    keyboard: true,
    riseOnHover: true,
  });
  marker.row = row;
  marker.on("add", () => {
    const node = marker.getElement();
    if (!node) return;
    node.classList.toggle("low-confidence", !row.has_sufficient_data);
    node.setAttribute("role", "button");
    node.setAttribute("aria-label", `${row.title || "Propiedad"}: ${discountText(row)}`);
  });
  marker.on("mouseover", () => showCard(marker, { pin: false }));
  marker.on("mouseout", scheduleHide);
  marker.on("click", (event) => {
    showCard(marker, { pin: true });
    if (event.originalEvent?.type?.startsWith("key")) popup.getElement()?.querySelector(".cta")?.focus();
  });
  return marker;
}

/* ---------- state, filters, rendering ---------- */

let rows = [];
let markers = [];
let evaluated = 0;
let latestObservation = null;
let firstFit = true;

function setOverlay(mode, text, retry = false) {
  const overlay = $("overlay");
  overlay.hidden = mode === "none";
  overlay.className = `overlay ${mode}`;
  $("overlay-text").textContent = text || "";
  $("retry").hidden = !retry;
}

function setStats(visible, unlocated) {
  const count = (rel) => visible.filter((m) => relation(m.row) === rel).length;
  const items = [
    ["Evaluadas", number(evaluated), ""],
    ["En el mapa", number(visible.length), ""],
    ["Por debajo", number(count("below")), "below"],
    ["Por encima", number(count("above")), "above"],
    ["Candidatas", number(visible.filter((m) => m.row.is_opportunity).length), ""],
  ];
  if (unlocated) items.push(["Sin ubicación", number(unlocated), ""]);
  if (latestObservation && !Number.isNaN(latestObservation.getTime())) {
    items.push(["Datos al", latestObservation.toLocaleDateString("es-AR", {
      timeZone: "America/Argentina/Buenos_Aires", day: "2-digit", month: "2-digit",
    }), ""]);
  }
  $("stats").replaceChildren(
    ...items.map(([label, value, cls]) => {
      const box = el("div", cls);
      box.append(el("dt", "", label), el("dd", "", value));
      return box;
    }),
  );
}

function currentFilters() {
  return {
    operation: $("f-operation").value,
    area: $("f-area").value,
    rel: $("f-relation").value,
    sufficient: $("f-sufficient").checked,
  };
}

function render() {
  const f = currentFilters();
  const visible = markers.filter(({ row }) =>
    (!f.operation || row.operation === f.operation) &&
    (!f.area || row.area_name === f.area) &&
    (!f.rel || relation(row) === f.rel) &&
    (!f.sufficient || row.has_sufficient_data));

  map.closePopup(popup);
  cluster.clearLayers();
  cluster.addLayers(visible);
  setStats(visible, rows.length - markers.length);

  if (!rows.length) {
    setOverlay("static", "Todavía no hay propiedades evaluadas. Ejecutá la recolección para generar datos.");
  } else if (!markers.length) {
    setOverlay("static", `Hay ${number(rows.length)} propiedades evaluadas, pero ninguna tiene ubicación en el mapa.`);
  } else if (!visible.length) {
    setOverlay("passive", "Ninguna propiedad coincide con los filtros.");
  } else {
    setOverlay("none");
    if (firstFit || f.operation || f.area) {
      map.fitBounds(cluster.getBounds(), { padding: [50, 50], maxZoom: 15 });
    }
    firstFit = false;
  }
}

function fillAreas() {
  const select = $("f-area");
  const areas = [...new Set(rows.map((r) => r.area_name).filter(Boolean))].sort((a, b) =>
    a.localeCompare(b, "es"));
  select.replaceChildren(new Option("Todas", ""), ...areas.map((a) => new Option(a, a)));
}

async function load() {
  setOverlay("loading", "Cargando propiedades…");
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 25000);
  try {
    const response = await fetch("/api/mapa", { signal: controller.signal });
    const payload = await response.json().catch(() => null);
    if (!response.ok) throw new Error(payload?.error || `Error ${response.status}`);
    if (!payload || !Array.isArray(payload.resultados)) throw new Error("Respuesta inesperada de la API.");
    rows = payload.resultados.filter((r) => r && typeof r === "object");
    evaluated = Number.isFinite(payload.evaluados) ? payload.evaluados : rows.length;
    latestObservation = payload.latest_observation_at ? new Date(payload.latest_observation_at) : null;
    markers = rows.filter(hasLocation).map(buildMarker);
    fillAreas();
    render();
  } catch (error) {
    const text = error.name === "AbortError" ? "La carga tardó demasiado." : error.message;
    setOverlay("static", `No se pudieron cargar las propiedades. ${text}`, true);
  } finally {
    clearTimeout(timeout);
  }
}

if (window.matchMedia("(max-width: 720px)").matches) $("legend").open = false;
$("filters").addEventListener("change", render);
$("filters").addEventListener("submit", (event) => event.preventDefault());
$("retry").addEventListener("click", load);
load();

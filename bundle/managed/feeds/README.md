# Regional feed additions

These 48 feeds add the 18 requested Saskatchewan and Alberta locations, the 17 missing Manitoba stations, five additional northern Ontario stations, and eight additional southern Ontario stations. Winnipeg remains in `cwxr-mb01.xml`, completing all 18 Manitoba stations in the referenced directory.

Feed IDs continue the existing province numbering. Files are enabled and retain the existing English routine playout, SAME originator, operator description, and Canadian alert filters. The directory loader derives IDs from filenames. These are source bundle files, not a live deployment.

## Sources and coverage

Historical callsigns and frequencies come from [William Hepburn's Weather Radio directory](https://www.dxinfocentre.com/wx.htm), dated March 29, 2026. Callsigns omit the presentation hyphen, matching existing metadata. Unlisted towns have no callsign or frequency assigned. These configurations do not assert active Environment Canada transmitters or reproduce former broadcast coverage.

City-page identifiers were checked against [ECCC's city-page API](https://api.weather.gc.ca/collections/citypageweather-realtime/items?f=json&limit=1000) on September 6, 2026. Canadian forecast-region and US county SAME identifiers were checked against `bundle/managed/alert_location_map.sqlite`. Coverage uses modern regional alert areas, with narrower catalog areas for small transmitter communities where available.

Where there is no matching city page, the inventory names the nearby product used. Burmis uses Crowsnest and Pincher Creek. Limestone Mountain uses Sundre and Rocky Mountain House for adjacent foothill coverage; these are not summit forecasts. Riverton uses Gimli, Woodridge uses Sprague, Falcon Lake uses Whiteshell, Neepawa uses Carberry, Reston uses Virden and Melita, Pointe du Bois uses Pinawa, St. Laurent uses Oak Point, Gull Lake uses Grand Beach, and Long Point uses Grand Rapids. No transmitter coordinates were inferred from city-page coordinates.

## US border coverage

Ten feeds include nearby border counties, selected for proximity across the border or connecting river. Each uses six-digit county SAME coverage and explicitly enables NWS CAP with `use_feed_locations="true"`. These are county alert additions, not US routine forecasts: the current ingest implementation only builds routine regional forecasts from ECCC city pages. County boundaries can extend well beyond the local border community. No RF reception contour is implied.

Kenora and Fort Frances use `America/Winnipeg`; the remaining Ontario additions use `America/Toronto`. Saskatchewan, Alberta, and Manitoba use their respective provincial timezones.

## Inventory

| Feed ID | Site | Historical callsign | ECCC forecast and observation products | US alert counties (SAME) |
| --- | --- | --- | --- | --- |
| cwxr-sk03 | Prince Albert | VAR551 | Prince Albert (sk-27) | None |
| cwxr-sk04 | North Battleford | Unlisted | North Battleford (sk-34) | None |
| cwxr-sk05 | La Ronge | CFJ262 | La Ronge (sk-38) | None |
| cwxr-sk06 | Kindersley | Unlisted | Kindersley (sk-21) | None |
| cwxr-sk07 | Swift Current | Unlisted | Swift Current (sk-41) | None |
| cwxr-sk08 | Maple Creek | Unlisted | Maple Creek (sk-16) | None |
| cwxr-sk09 | Yorkton | Unlisted | Yorkton (sk-33) | None |
| cwxr-sk10 | Fort Qu'Appelle | CHZ715 | Fort Qu'Appelle (sk-5) | None |
| cwxr-sk11 | Watrous | Unlisted | Watrous (sk-49) | None |
| cwxr-sk12 | Broadview | VCB462 | Broadview (sk-48) | None |
| cwxr-sk13 | Estevan | VAM595 | Estevan (sk-53), Oxbow (sk-10) | Divide, ND (038023); Burke, ND (038013) |
| cwxr-ab05 | Lethbridge | Unlisted | Lethbridge (ab-30) | None |
| cwxr-ab06 | Medicine Hat | VBK616 | Medicine Hat (ab-51) | None |
| cwxr-ab07 | Burmis | VBX254 | Crowsnest (ab-17), Pincher Creek (ab-46) | None |
| cwxr-ab08 | Cold Lake | VFZ535 | Cold Lake (ab-23) | None |
| cwxr-ab09 | Fort McMurray | CFA340 | Fort McMurray (ab-20) | None |
| cwxr-ab10 | Limestone Mountain | VDA280 | Sundre (ab-53), Rocky Mountain House (ab-16) | None |
| cwxr-ab11 | Canmore | Unlisted | Canmore (ab-3) | None |
| cwxr-mb02 | Churchill | CHR943 | Churchill (mb-42) | None |
| cwxr-mb03 | Portage la Prairie | CKE695 | Portage la Prairie (mb-29) | None |
| cwxr-mb04 | Riverton | XLF471 | Gimli (mb-62) | None |
| cwxr-mb05 | Thompson | VXI858 | Thompson (mb-34) | None |
| cwxr-mb06 | Woodridge | CGN886 | Sprague (mb-23) | Roseau, MN (027135) |
| cwxr-mb07 | Altona | VFN684 | Altona (mb-3) | Pembina, ND (038067); Kittson, MN (027069) |
| cwxr-mb08 | Falcon Lake | VXE212 | Whiteshell (mb-8) | None |
| cwxr-mb09 | Neepawa | CGN883 | Carberry (mb-35) | None |
| cwxr-mb10 | Reston | VXK206 | Virden (mb-20), Melita (mb-45) | None |
| cwxr-mb11 | Pointe du Bois | VXG567 | Pinawa (mb-44) | None |
| cwxr-mb12 | St. Laurent | CGT911 | Oak Point (mb-14) | None |
| cwxr-mb13 | Steinbach | VFN683 | Steinbach (mb-13) | None |
| cwxr-mb14 | Gull Lake | CGN875 | Grand Beach (mb-9) | None |
| cwxr-mb15 | Brandon | VAO302 | Brandon (mb-52) | None |
| cwxr-mb16 | Dauphin | VBA814 | Dauphin (mb-58) | None |
| cwxr-mb17 | Long Point | VCI386 | Grand Rapids (mb-57) | None |
| cwxr-mb18 | Winkler | VXM345 | Winkler (mb-26) | Pembina, ND (038067) |
| cwxr-on08 | Kenora | XLJ890 | Kenora (on-96), Sioux Narrows (on-166) | None |
| cwxr-on09 | Fort Frances | VDB224 | Fort Frances (on-159) | Koochiching, MN (027071) |
| cwxr-on10 | Sault Ste. Marie | XMJ373 | Sault Ste. Marie (on-162) | Chippewa, MI (026033) |
| cwxr-on11 | North Bay | XLJ893 | North Bay (on-139) | None |
| cwxr-on12 | Timmins | VDB886 | Timmins (on-127) | None |
| cwxr-on13 | Niagara Falls | VAD320 | Niagara Falls (on-125) | Niagara, NY (036063) |
| cwxr-on14 | Kingston | XJV363 | Kingston (on-69) | Jefferson, NY (036045) |
| cwxr-on15 | Brockville | VFK721 | Brockville (on-8) | St. Lawrence, NY (036089) |
| cwxr-on16 | Belleville | VFK720 | Belleville (on-3) | None |
| cwxr-on17 | Sarnia - Oil Springs | XJV492 | Sarnia (on-147) | St. Clair, MI (026147) |
| cwxr-on18 | Collingwood | XMJ316 | Collingwood (on-150) | None |
| cwxr-on19 | Orillia | VBV562 | Orillia (on-13) | None |
| cwxr-on20 | Goderich | XLT839 | Goderich (on-160) | None |

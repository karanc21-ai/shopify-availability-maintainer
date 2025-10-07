# inside the loop where you computed `delta` for the US variant:
if delta != 0:
    # Get US product.custom.sku from the variant
    us_v_data = gql(US_DOMAIN, US_TOKEN, QUERY_US_VARIANT_WITH_PRODUCT_SKU, {"id": vgid})
    us_v = (us_v_data.get("productVariant") or {})
    us_prod = (us_v.get("product") or {})
    raw_prod_sku = ((us_prod.get("skuMeta") or {}).get("value") or "").strip()
    prod_sku_norm = normalize_sku(raw_prod_sku)

    if not prod_sku_norm:
        log_row("US→IN", "US", "**WARN_NO_PRODUCT_CUSTOM_SKU_US**",
                variant_id=vid, sku="", delta=delta,
                message="US product has no custom.sku; cannot mirror to India")
    else:
        in_prod, in_inv_item_id = find_india_product_by_custom_sku(
            prod_sku_norm,
            only_active=os.getenv("ONLY_ACTIVE_FOR_MAPPING","1") == "1"
        )
        if not in_prod:
            log_row("US→IN", "US", "**WARN_PRODUCT_SKU_NOT_FOUND_IN_INDIA**",
                    variant_id=vid, sku=raw_prod_sku, delta=delta,
                    message=f"US product.custom.sku={raw_prod_sku} not found in India")
        elif in_inv_item_id is None:
            log_row("US→IN", "IN", "**WARN_NO_TRACKED_VARIANT_IN_INDIA**",
                    variant_id=vid, sku=raw_prod_sku, delta=delta,
                    message=f"India product has no tracked variant to adjust | pid={gid_num(in_prod.get('id',''))}")
        else:
            try:
                rest_adjust_inventory(IN_DOMAIN, IN_TOKEN, in_inv_item_id, int(IN_LOCATION_ID), delta)
                log_row("US→IN", "IN", "APPLIED_DELTA",
                        variant_id=vid, sku=raw_prod_sku, delta=delta,
                        message=f"Adjusted IN inventory_item_id={in_inv_item_id} by {delta} (via product.custom.sku)")
                time.sleep(MUTATION_SLEEP_SEC)
            except Exception as e:
                log_row("US→IN", "IN", "ERROR_APPLYING_DELTA",
                        variant_id=vid, sku=raw_prod_sku, delta=delta, message=str(e))

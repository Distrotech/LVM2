/* FIXME: REMOVEME: development output */
static void dump_area(const char *msg, struct lv_segment *seg, unsigned a)
{
	struct physical_volume *pv;
	struct pv_segment *pvseg;
	struct logical_volume *lv;

	if (seg_type(seg, a) == AREA_PV) {
		pv = seg_pv(seg, a);
		pvseg = seg_pvseg(seg, a);

		printf("pv[(%u]=%p", a, pv);
		printf(" pe=%u", pvseg->pe);
		printf(" len=%u\n", pvseg->len);

	} else if (seg_type(seg, a) == AREA_LV) {
		lv = seg_lv(seg, a);

		printf("seg_lv[seg, %u]=%p",	a, lv);
		printf(" name=%s\n",lv->name);

	} else
		printf("v[%u]=AREA_UNASSIGNED\n", a);
}

static void dump_lv(const char *msg, struct logical_volume *lv);
static void dump_seg(const char *msg, struct lv_segment *seg, int self)
{
	unsigned a;

	printf("%s seg=%p", msg, seg);
	printf(" name=%s", 		lvseg_name(seg));
	printf(" lv->name=%s",		seg->lv->name);
	printf(" lv=%p", 		seg->lv);
	printf(" le=%u", 		seg->le);
	printf(" len=%u", 		seg->len);
	printf(" status=%llu", 		(unsigned long long) seg->status);
	printf(" stripe_size=%u", 	seg->stripe_size);
	printf(" area_count=%u",	seg->area_count);
	printf(" area_len=%u", 		seg->area_len);
	printf(" chunk_size=%u", 	seg->chunk_size);
	printf(" region_size=%u\n", 	seg->region_size);

	if (self)
		return;

	for (a = 0; a < seg->area_count; a++) {
		if (seg->meta_areas && seg_metatype(seg, a) == AREA_LV)
			dump_lv(msg, seg_metalv(seg, a));
		else {
			dump_area(msg, seg, a);
			continue;
		}

		if (seg_type(seg, a) == AREA_LV)
			dump_lv(msg, seg_lv(seg, a));
		else
			dump_area(msg, seg, a);
	}
}

static void dump_lv(const char *msg, struct logical_volume *lv)
{
	struct lv_segment *seg;

	printf("---> %s lv=%p <---\n", msg, lv);
	printf("lv->name=%s",	lv->name);
	printf(" le_count=%u",	lv->le_count);
	printf(" size=%llu",	(unsigned long long) lv->size);
	printf(" status=%lX\n",	lv->status);

	dm_list_iterate_items(seg, &lv->segments)
		dump_seg(msg, seg, 0);
}

#include "quark-internal.h"

#pragma librcd

static json_value_t objid(qk_ctx_t* ctx, void* ptr, fstr_t type) {
    size_t n = ((size_t) ptr - (size_t) acid_memory(ctx->ah).str) >> QK_PAGE_SIZE_2E;
    return jstr(sconc(type, "/", fss(fstr_from_uint(n, 16))));
}

static void qk_vis_part(qk_ctx_t* ctx, uint8_t level, qk_part_t* part, json_value_t nodes, json_value_t edges, json_value_t from_id) {
    json_value_t part_id = objid(ctx, part, "part");
    json_append(nodes, jobj_new(
        {"group", jstr("partition")},
        {"id", part_id},
        {"label", part_id},
        {"level", jnum(level)},
    ));
    json_append(edges, jobj_new(
        {"from", from_id},
        {"to", part_id},
    ));
    qk_idx_t* idx0 = qk_part_get_idx0(part);
    qk_idx_t* idxE = idx0 + part->n_keys;
    for (qk_idx_t* idxC = idx0; idxC < idxE; idxC++) {
        json_value_t node_id = objid(ctx, idxC, "node");
        json_append(edges, jobj_new(
            {"from", part_id},
            {"to", node_id},
        ));
        fstr_t key = qk_idx_get_key(idxC);
        if (level > 0) {
            json_append(nodes, jobj_new(
                {"group", jstr("key-node")},
                {"id", node_id},
                {"label", jstr(key)},
                {"level", jnum(level)},
            ));
            qk_part_t* cpart = *qk_idx1_get_down_ptr(idxC);
            qk_vis_part(ctx, level - 1, cpart, nodes, edges, node_id);
        } else {
            fstr_t value = qk_idx0_get_value(idxC);
            json_append(nodes, jobj_new(
                {"group", jstr("value-node")},
                {"id", node_id},
                {"label", jstr(concs(key, "\n", value))},
                {"level", jnum(0)},
            ));
        }
    }
}

void qk_vis(qk_ctx_t* ctx, rio_t* out_h) { sub_heap {
    qk_hdr_t* hdr = ctx->hdr;
    json_value_t nodes = jarr_new();
    json_value_t edges = jarr_new();
    for (uint8_t i_lvl = 0; i_lvl < LENGTHOF(hdr->root); i_lvl++) {
        json_value_t root_id = jstr(concs("root#", i_lvl));
        json_append(nodes, jobj_new(
            {"group", jstr("root")},
            {"id", root_id},
            {"label", jstr(concs("level #", i_lvl, " root"))},
            {"level", jnum(i_lvl)},
        ));
        qk_vis_part(ctx, i_lvl, hdr->root[i_lvl], nodes, edges, root_id);
    }
    for (uint8_t class = 0; class < hdr->free_end_class; class++) {
        json_value_t list_id = jstr(concs("flist#", class));
        json_value_t level = jnum(LENGTHOF(hdr->root) + class);
        json_append(nodes, jobj_new(
            {"group", jstr("free-list")},
            {"id", list_id},
            {"label", jstr(concs("free list #", class, "\n", (1 << (QK_PAGE_SIZE_2E + class)), "b"))},
            {"level", level},
        ));
        void** chunk = hdr->free_list[class];
        json_value_t prev_id = list_id;
        while (chunk != 0) {
            json_value_t cur_id = objid(ctx, chunk, "chunk");
            json_append(edges, jobj_new(
                {"from", prev_id},
                {"to", cur_id},
            ));
            json_append(nodes, jobj_new(
                {"group", jstr("free-chunk")},
                {"id", cur_id},
                {"label", cur_id},
                {"level", level},
            ));
            prev_id = cur_id;
            chunk = *chunk;
        }
    }
    extern const fstr_t vis_tpl;
    fstr_t html = fss(fstr_replace(vis_tpl, "$DATA", fss(json_stringify_pretty(jobj_new(
        {"nodes", nodes},
        {"edges", edges},
    )))));
    rio_write(out_h, html);
}}

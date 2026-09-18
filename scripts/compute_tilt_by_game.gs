/**
 * Tilt By Game v4 — self-contained in-sheet pitcher-split F5 tilt (Fable 2026-09-15).
 * ---------------------------------------------------------------------------------------
 * v4 (2026-09-17): ROBUST FETCH. v2/v3 fired ~2×N statsapi requests in one fetchAll burst
 * and, when statsapi throttled a request (non-JSON error page), JSON.parse THREW and the
 * try/catch SILENTLY DROPPED that starter — so the OPPOSING team's tilt hard-zeroed with
 * no warning (e.g. Walbert Urena 2026-09-17: away_tilt 0 despite clean career splits).
 * Fixes:
 *   - non-throwing parse (bad/non-200 response -> null, never throws);
 *   - ONE retry pass for any starter whose bio/stats didn't come back;
 *   - never drop an id'd starter: a still-missing splits fetch falls back to the league
 *     hand-prior (priorOnly) instead of vanishing, so tilt is small+handedness-correct, not 0;
 *   - toast lists any starters that ended up on the prior, so a miss is visible.
 *
 * v2 math unchanged: CAREER splits (careerStatSplits, TBF-weighted); shrink L/R DELTA toward
 * the league platoon prior (K=700 on the smaller side's TBF); FLOOR to prior if either career
 * side < 150 TBF; PA-true slot weights; openers zeroed; switch hitters bat opposite the SP.
 *
 * Reads:  Lineups (Team, order 1-9 batters/10 SP, Player, GameId), Handedness (id|name|bats),
 *         Projections Calculations (GameID -> Away/Home).
 * Writes: "Tilt By Game": GameID | away_tilt | home_tilt (header row 2, data row 3+).
 * Apply:  F5_team_runs += 3.3 * VLOOKUP(GameID,'Tilt By Game'!$A$3:$C, 2/3, FALSE).
 */

var SEASON = 2026;
var OUT_TAB = 'Tilt By Game';
var LINEUPS_TAB = 'Lineups';
var HAND_TAB = 'Handedness';
var CALC_TAB = 'Projections Calculations';
var SHR_K = 75.0;               // per-side sample weight (v1), ~1 with career TBF
var DELTA_K = 700.0;            // v2: delta shrinkage toward league prior
var LG_RHP = 0.1246;            // 2026 league platoon FIP/PA delta (vsL - vsR), RHP
var LG_LHP = -0.1970;           // LHP analog
var FLOOR_TBF = 150;            // v2: thin career side (< 150 TBF) => no reliable INDIVIDUAL read
var PRIOR_FALLBACK = true;      // v3: thin/rookie/failed arms fall back to the league hand-prior
var PRIOR_SHR = 0.5;            // confidence multiplier on the prior-only tilt (tunable; Fable)
var SLOT_WT = [2.91, 2.78, 2.65, 2.51, 2.38, 2.27, 2.18, 2.10, 2.02];  // PA-true

function _norm(s) {
  if (s == null) return '';
  var x = String(s).normalize('NFKD'), out = '';
  for (var i = 0; i < x.length; i++) { var c = x.charCodeAt(i); if (c < 768 || c > 879) out += x.charAt(i); }  // drop combining accents (ASCII-only, paste-safe)
  return out.replace(/\s+(jr|sr|ii|iii|iv|v)\.?$/i, '')
    .replace(/['.]/g, '').replace(/\s+/g, ' ').trim().toLowerCase();
}
function _fiKey(nk) {
  var p = String(nk).split(' ');
  if (p.length < 2) return '';
  return p[0].charAt(0) + ' ' + p[p.length - 1];   // "zach thornton" -> "z thornton"
}
// StatsAPI name search fallback for a starter not in Handedness. Accepts only a
// UNIQUE active pitcher matching the full norm or first-initial+lastname. Returns
// {id, throws} or null (null -> caller uses the league prior, never a hard 0).
function _searchPitcher(name) {
  try {
    var last = String(name).trim().split(/\s+/).pop();
    var r = UrlFetchApp.fetch('https://statsapi.mlb.com/api/v1/people/search?names=' +
      encodeURIComponent(last), {muteHttpExceptions: true});
    if (r.getResponseCode() !== 200) return null;
    var ppl = (JSON.parse(r.getContentText()).people) || [];
    var want = _norm(name), wantFi = _fiKey(want);
    var cands = ppl.filter(function (p) { return p.active && ((p.primaryPosition || {}).code === '1'); })
      .map(function (p) { return {id: p.id, nm: _norm(p.fullName || ''), throws: (p.pitchHand || {}).code || 'R'}; });
    var exact = cands.filter(function (c) { return c.nm === want; });
    if (exact.length === 1) return exact[0];
    var fi = cands.filter(function (c) { return _fiKey(c.nm) === wantFi; });
    if (fi.length === 1) return fi[0];
    return null;
  } catch (e) { return null; }
}
function _n(x) { var v = parseFloat(x); return isNaN(v) ? 0 : v; }
function _fip(st) {
  if (!st) return null;
  var bf = _n(st.battersFaced);
  if (bf <= 0) return null;
  return (13 * _n(st.homeRuns) + 3 * _n(st.baseOnBalls) - 2 * _n(st.strikeOuts)) / bf;
}
// non-throwing JSON parse: bad body / non-200 -> null (never throws, so an SP is never dropped)
function _pj(r) {
  try { return (r && r.getResponseCode() === 200) ? JSON.parse(r.getContentText()) : null; }
  catch (e) { return null; }
}
function _bioReq(id) { return {url: 'https://statsapi.mlb.com/api/v1/people/' + id, muteHttpExceptions: true}; }
function _stReq(id)  { return {url: 'https://statsapi.mlb.com/api/v1/people/' + id +
  '/stats?stats=careerStatSplits,season&group=pitching&season=' + SEASON + '&sitCodes=vr,vl',
  muteHttpExceptions: true}; }

function computeTiltByGame() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();

  // 1) Handedness: name -> bats, name -> mlbam_id
  var HAND = {}, NAMEID = {}, FI = {};
  var hv = ss.getSheetByName(HAND_TAB).getDataRange().getValues();
  for (var i = 0; i < hv.length; i++) {
    var id = hv[i][0], nm = hv[i][1], bt = hv[i][2];
    if (nm) {
      var kn = _norm(nm);
      if (bt) HAND[kn] = String(bt).trim().toUpperCase();
      if (id !== '' && id != null) {
        NAMEID[kn] = id;
        var _fk = _fiKey(kn);
        if (_fk) FI[_fk] = (FI[_fk] === undefined) ? id : 'AMBIG';   // unique-only fallback
      }
    }
  }

  // 2) GameID -> {away, home}
  var GAME = {};
  var cv = ss.getSheetByName(CALC_TAB).getDataRange().getValues();
  for (var r = 4; r < cv.length; r++) {
    var aw = cv[r][1], hm = cv[r][2], gid = cv[r][3];
    if (gid !== '' && gid != null && aw && hm)
      GAME[String(gid).trim()] = {away: String(aw).trim(), home: String(hm).trim()};
  }

  // 3) Lineups -> byGame[gid][team] = {batters, sp}; collect SP names
  var byGame = {}, spNames = {};
  var lv = ss.getSheetByName(LINEUPS_TAB).getDataRange().getValues();
  for (var j = 1; j < lv.length; j++) {
    var team = lv[j][1], order = lv[j][2], player = lv[j][4], g = lv[j][5];
    if (!team || g === '' || g == null || player === '' || order === '' || order == null) continue;
    var gg = String(g).trim(), t = String(team).trim(), o = Number(order);
    byGame[gg] = byGame[gg] || {};
    byGame[gg][t] = byGame[gg][t] || {batters: {}, sp: null};
    if (o >= 1 && o <= 9) byGame[gg][t].batters[o] = player;
    else if (o === 10) { byGame[gg][t].sp = player; spNames[_norm(player)] = player; }
  }

  // 3b) resolve SP names missing from Handedness: unique first-initial+lastname
  //     (RotoWire "Zach" -> MLB "Zac" Thornton), then StatsAPI name search (append
  //     new arms to Handedness so next run is direct). Unresolved -> league prior below.
  var _newHand = [];
  Object.keys(spNames).forEach(function (k) {
    if (NAMEID[k] != null) return;
    var alt = _fiKey(k);
    if (alt && FI[alt] != null && FI[alt] !== 'AMBIG') { NAMEID[k] = FI[alt]; return; }
    var found = _searchPitcher(spNames[k]);
    if (found && found.id) { NAMEID[k] = found.id; _newHand.push([found.id, spNames[k], (found.throws || 'R'), 'P']); }
  });
  if (_newHand.length) {
    try {
      var _hs = ss.getSheetByName(HAND_TAB);
      _hs.getRange(_hs.getLastRow() + 1, 1, _newHand.length, 4).setValues(_newHand);
    } catch (e) {}
  }

  // 4) fetch career splits + bio for every id'd starter — with a retry pass and no silent drops
  var keys = Object.keys(spNames).filter(function (k) { return NAMEID[k] != null; });
  function fetchPairs(ks) {
    var reqs = [];
    ks.forEach(function (k) { reqs.push(_bioReq(NAMEID[k])); reqs.push(_stReq(NAMEID[k])); });
    return reqs.length ? UrlFetchApp.fetchAll(reqs) : [];
  }
  var got = {};                                   // key -> {bio, st}
  var resp = fetchPairs(keys);
  var failed = [];
  for (var m = 0; m < keys.length; m++) {
    var bio1 = _pj(resp[2 * m]), st1 = _pj(resp[2 * m + 1]);
    got[keys[m]] = {bio: bio1, st: st1};
    if (!bio1 || !st1) failed.push(keys[m]);       // throttled/non-JSON -> retry
  }
  if (failed.length) {
    Utilities.sleep(900);
    var r2 = fetchPairs(failed);
    for (var f = 0; f < failed.length; f++) {
      var kf = failed[f];
      got[kf] = {bio: _pj(r2[2 * f]) || got[kf].bio, st: _pj(r2[2 * f + 1]) || got[kf].st};
    }
  }

  var SP = {}, onPrior = [];
  keys.forEach(function (k) {
    var P = got[k] || {};
    var bio = (((P.bio || {}).people) || [{}])[0] || {};
    var hand = ((bio.pitchHand || {}).code) || 'R';
    var groups = ((P.st || {}).stats) || [];
    var vr = null, vl = null, season = null;
    groups.forEach(function (grp) {
      var typ = (grp.type || {}).displayName || (grp.type || {}).code;
      (grp.splits || []).forEach(function (s) {
        var code = (s.split || {}).code;
        if (typ === 'careerStatSplits' && code === 'vr') vr = s.stat;
        else if (typ === 'careerStatSplits' && code === 'vl') vl = s.stat;
        else if (typ === 'season' && !season) season = s.stat;
      });
    });
    var mL = _fip(vl), mR = _fip(vr);
    var tL = _n((vl || {}).battersFaced), tR = _n((vr || {}).battersFaced);
    var lg = (hand === 'R') ? LG_RHP : LG_LHP;
    // thin OR the splits fetch never came back => league hand-prior (never a silent drop)
    var thin = (!P.st || mL == null || mR == null || tL < FLOOR_TBF || tR < FLOOR_TBF);
    var priorOnly = PRIOR_FALLBACK && thin;
    if (!P.st) onPrior.push(spNames[k] + (P.bio ? '' : '?'));   // '?' = hand defaulted
    var T = tL + tR;
    var mo = T > 0 ? (tL * (mL || 0) + tR * (mR || 0)) / T : 0;
    var deltaRaw = (mL != null && mR != null) ? (mL - mR) : lg;
    var tbfMin = Math.min(tL, tR);
    var w = tbfMin / (tbfMin + DELTA_K);
    var delta = w * deltaRaw + (1 - w) * lg;
    var sideL = mo + (tR / (T || 1)) * delta;
    var sideR = mo - (tL / (T || 1)) * delta;
    var gs = _n((season || {}).gamesStarted), ip = _n((season || {}).inningsPitched);
    var opener = (gs >= 3 && (ip / gs) < 2.5);
    SP[k] = {hand: hand, opener: opener, floored: thin, priorOnly: priorOnly,
             priorDelta: lg, sideL: sideL, sideR: sideR, mo: mo, tl: tL, tr: tR};
  });
  // Unresolved starters (no id anywhere): still emit a small league-prior SP so the
  // opposing tilt is handedness-neutral+nonzero rather than a hard 0.
  Object.keys(spNames).forEach(function (k) {
    if (SP[k]) return;
    SP[k] = {hand: 'R', opener: false, floored: true, priorOnly: true,
             priorDelta: LG_RHP, sideL: 0, sideR: 0, mo: 0, tl: 0, tr: 0};
    onPrior.push(spNames[k] + '??');   // ?? = fully unresolved (default RHP prior)
  });

  function tilt(batters, oppSP) {
    if (!oppSP || oppSP.opener) return 0;
    var num = 0, den = 0;
    for (var slot = 1; slot <= 9; slot++) {
      var nm = batters[slot]; if (!nm) continue;
      var bats = HAND[_norm(nm)] || 'R';
      var side;
      if (bats === 'S') side = (oppSP.hand === 'R') ? 'L' : 'R';   // switch bats opposite SP
      else side = (bats === 'R') ? 'R' : 'L';
      var wt = SLOT_WT[slot - 1], contrib;
      if (oppSP.priorOnly) {
        var d = oppSP.priorDelta;
        contrib = PRIOR_SHR * ((side === 'L') ? (d / 2) : (-d / 2));
      } else {
        var sideEst = (side === 'L') ? oppSP.sideL : oppSP.sideR;
        var tbf = (side === 'L') ? oppSP.tl : oppSP.tr;
        var shr = tbf / (tbf + SHR_K);
        contrib = shr * (sideEst - oppSP.mo);
      }
      num += wt * contrib;
      den += wt;
    }
    return den ? num / den : 0;
  }

  // 5) per game
  var rows = [];
  Object.keys(GAME).forEach(function (gid) {
    var away = GAME[gid].away, home = GAME[gid].home;
    var aL = (byGame[gid] || {})[away], hL = (byGame[gid] || {})[home];
    if (!aL || !hL) return;
    var awaySP = aL.sp ? SP[_norm(aL.sp)] : null;
    var homeSP = hL.sp ? SP[_norm(hL.sp)] : null;
    var aT = tilt(aL.batters, homeSP);   // away bats vs HOME sp
    var hT = tilt(hL.batters, awaySP);   // home bats vs AWAY sp
    rows.push([Number(gid), Math.round(aT * 1e5) / 1e5, Math.round(hT * 1e5) / 1e5]);
  });
  rows.sort(function (a, b) { return a[0] - b[0]; });

  // 6) write Tilt By Game (header row 2, data row 3+)
  var sh = ss.getSheetByName(OUT_TAB) || ss.insertSheet(OUT_TAB);
  var last = sh.getLastRow();
  if (last >= 2) sh.getRange(2, 1, last - 1, 3).clearContent();
  sh.getRange(2, 1, 1, 3).setValues([['GameID', 'away_tilt', 'home_tilt']]);
  if (rows.length) sh.getRange(3, 1, rows.length, 3).setValues(rows);

  try { var _cb = ss.getSheetByName('Dashboard').getRange('M2'); if (!_cb.getDataValidation()) _cb.insertCheckboxes(); } catch (e) {}

  var nz = rows.filter(function (x) { return x[1] !== 0 || x[2] !== 0; }).length;
  var priorN = Object.keys(SP).filter(function (k) { return SP[k].priorOnly; }).length;
  var msg = 'Tilt v4: ' + rows.length + ' games, ' + nz + ' nonzero (' + keys.length +
    ' SPs, ' + priorN + ' on league-prior)';
  if (onPrior.length) msg += ' | fetch-miss->prior: ' + onPrior.join(', ');
  ss.toast(msg, 'Tilt By Game', 8);
}

/** Checkbox hook — tick Tilt By Game!A1 (or Dashboard!M2 via onEdit) to recompute. */
function wtiltGameOnCheckboxEdit(e) {
  if (!e || !e.range) return;
  var _sh = e.range.getSheet().getName(), _a1 = e.range.getA1Notation();
  if (!((_sh === OUT_TAB && _a1 === 'A1') || (_sh === 'Dashboard' && _a1 === 'M2'))) return;
  if (e.value !== 'TRUE') return;
  try { computeTiltByGame(); } finally { e.range.setValue(false); }
}

function setupTiltCheckbox() {
  var sh = SpreadsheetApp.getActive().getSheetByName('Dashboard');
  sh.getRange('M2').insertCheckboxes();
  SpreadsheetApp.getActive().toast('Tilt checkbox added at Dashboard!M2', 'Setup', 4);
}

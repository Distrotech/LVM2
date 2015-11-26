/*
 * Copyright (C) 2015 Red Hat, Inc. All rights reserved.
 *
 * This file is part of LVM2.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License v.2.1.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

/*
 * This file defines the fields (columns) for the command status reporting.
 *
 * The preferred order of the field descriptions in the help text
 * determines the order the entries appear in this file.
 *
 * When adding new entries take care to use the existing style.
 * Displayed fields names normally have a type prefix and use underscores.
 * Field-specific internal functions names normally match the displayed
 * field names but without underscores.
 * Help text ends with a full stop.
 */

/* *INDENT-OFF* */
FIELD(CMDSTATUS, cmd_status, STR, "MsgType", type, 10, uint32, status_type, "Command Status Type.", 0)
FIELD(CMDSTATUS, cmd_status, STR, "Msg", msg, 10, string, status_message, "Command Status Message.", 0)
/* *INDENT-ON* */

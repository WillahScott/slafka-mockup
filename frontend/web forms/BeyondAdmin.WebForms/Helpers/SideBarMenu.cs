using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace BeyondAdmin.WebForms.Helpers
{
    [ControlValueProperty("SelectedValue")]
    [DefaultEvent("MenuItemClick")]
    [SupportsEventValidation]
    [ToolboxData("<{0}:SideBarMenu runat=\"server\"></{0}:SideBarMenu>")]
    public sealed class SideBarMenu : Menu
    {
        public override void RenderBeginTag(HtmlTextWriter writer)
        {
            // don't call base.RenderBeginTag()
        }

        public override void RenderEndTag(HtmlTextWriter writer)
        {
            // don't call base.RenderEndTag()
        }

        protected override void OnPreRender(EventArgs e)
        {
            // don't call base.OnPreRender(e);
            this.EnsureDataBound();
        }

        protected override void Render(HtmlTextWriter writer)
        {
            this.BuildItems(writer, this.Items, true);
        }

        protected override void EnsureDataBound()
        {
            base.EnsureDataBound();
        }

        private void BuildItems(HtmlTextWriter writer, MenuItemCollection items, bool isRoot = false)
        {
            if (items.Count <= 0)
            {
                return;
            }

            string cssClass = "submenu";

            if (isRoot)
            {
                cssClass = "nav sidebar-menu";
                if (!string.IsNullOrEmpty(this.CssClass))
                {
                    cssClass += " " + this.CssClass;
                }
            }

            writer.AddAttribute(HtmlTextWriterAttribute.Class, cssClass);
            writer.RenderBeginTag(HtmlTextWriterTag.Ul);

            if (isRoot)
                items = items[0].ChildItems;

            foreach (MenuItem item in items)
            {
                this.BuildItem(writer, item);
            }

            writer.RenderEndTag(); // </ul>
        }

        private void BuildItem(HtmlTextWriter writer, MenuItem item)
        {
            if (item == null)
            {
                throw new ArgumentNullException("item");
            }

            if (writer == null)
            {
                throw new ArgumentNullException("writer");
            }

            if (item.Selected)
                writer.AddAttribute(HtmlTextWriterAttribute.Class, "active");

            foreach (var child in item.ChildItems)
            {
                if (((MenuItem)child).Selected)
                    writer.AddAttribute(HtmlTextWriterAttribute.Class, "active open");

                foreach (var nextChild in ((MenuItem)child).ChildItems)
                {
                    if (((MenuItem)nextChild).Selected)
                        writer.AddAttribute(HtmlTextWriterAttribute.Class, "active open");

                }
            }
            writer.RenderBeginTag(HtmlTextWriterTag.Li);

            if (this.IsLink(item))
            {
                this.RenderLink(writer, item);
            }
            else if (this.HasChildren(item))
            {
                this.RenderDropDown(writer, item);
            }
            else
            {
                writer.RenderBeginTag(HtmlTextWriterTag.A);
                writer.Write(item.Text);
                writer.RenderEndTag();
            }

            writer.RenderEndTag(); // </li>
        }

        private bool HasChildren(MenuItem item)
        {
            if (item == null)
            {
                throw new ArgumentNullException("item");
            }

            return item.ChildItems.Count > 0;
        }

        private bool IsLink(MenuItem item)
        {
            if (item == null)
            {
                throw new ArgumentNullException("item");
            }

            return item.Enabled && !string.IsNullOrEmpty(item.NavigateUrl);
        }

        private void RenderLink(HtmlTextWriter writer, MenuItem item)
        {
            if (item == null)
            {
                throw new ArgumentNullException("item");
            }

            if (writer == null)
            {
                throw new ArgumentNullException("writer");
            }

            string href = !string.IsNullOrEmpty(item.NavigateUrl)
                    ? this.Page.Server.HtmlEncode(this.ResolveClientUrl(item.NavigateUrl))
                    : this.Page.ClientScript.GetPostBackClientHyperlink(
                        this,
                        "b" + item.ValuePath.Replace(this.PathSeparator.ToString(), "\\"),
                        true);
            writer.AddAttribute(HtmlTextWriterAttribute.Href, href);

            string toolTip = !string.IsNullOrEmpty(item.ToolTip)
                ? item.ToolTip
                : item.Text;
            writer.AddAttribute(HtmlTextWriterAttribute.Title, toolTip);

            writer.RenderBeginTag(HtmlTextWriterTag.A);

            if (!string.IsNullOrEmpty(item.ImageUrl))
            {
                writer.AddAttribute(HtmlTextWriterAttribute.Class, "menu-icon " + item.ImageUrl);
                writer.RenderBeginTag(HtmlTextWriterTag.I);
                writer.RenderEndTag(); // </i>
            }
            writer.AddAttribute(HtmlTextWriterAttribute.Class, "menu-text");
            writer.RenderBeginTag(HtmlTextWriterTag.Span);
            writer.Write(item.Text);
            writer.RenderEndTag(); // </span>
            writer.RenderEndTag(); // </a>
        }

        private void RenderDropDown(HtmlTextWriter writer, MenuItem item)
        {
            if (item == null)
            {
                throw new ArgumentNullException("item");
            }

            if (writer == null)
            {
                throw new ArgumentNullException("writer");
            }

            writer.AddAttribute(HtmlTextWriterAttribute.Href, "#");
            writer.AddAttribute(HtmlTextWriterAttribute.Class, "menu-dropdown");
            writer.RenderBeginTag(HtmlTextWriterTag.A);

            if (!string.IsNullOrEmpty(item.ImageUrl))
            {
                writer.AddAttribute(HtmlTextWriterAttribute.Class, "menu-icon " + item.ImageUrl);
                writer.RenderBeginTag(HtmlTextWriterTag.I);
                writer.RenderEndTag(); // </i>
            }
            writer.AddAttribute(HtmlTextWriterAttribute.Class, "menu-text");
            writer.RenderBeginTag(HtmlTextWriterTag.Span);
            writer.Write(item.Text);
            writer.RenderEndTag(); // </span>
            writer.AddAttribute(HtmlTextWriterAttribute.Class, "menu-expand");
            writer.RenderBeginTag(HtmlTextWriterTag.I);
            writer.RenderEndTag(); // </b>          

            writer.RenderEndTag(); // </a>

            this.BuildItems(writer, item.ChildItems);
        }
    }

}